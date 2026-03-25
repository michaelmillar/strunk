use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;
use strunk::config::StrunkConfig;
use strunk::Strunk;

fn test_config() -> Option<StrunkConfig> {
    let url = std::env::var("DATABASE_URL").ok()?;
    Some(StrunkConfig {
        database_url: url,
        poll_interval: Duration::from_millis(50),
        relay_batch_size: 100,
        reaper_retention_delivered: Duration::from_secs(1),
        reaper_retention_dead: Duration::from_secs(1),
        reaper_batch_size: 100,
        reaper_interval: Duration::from_secs(1),
    })
}

async fn setup() -> Option<Strunk> {
    let config = test_config()?;
    let strunk = Strunk::new(config).await.ok()?;
    strunk.migrate().await.ok()?;
    cleanup(strunk.pool()).await;
    Some(strunk)
}

async fn cleanup(pool: &PgPool) {
    sqlx::query("DELETE FROM strunk_outbox").execute(pool).await.ok();
    sqlx::query("DELETE FROM strunk_subscribers").execute(pool).await.ok();
    sqlx::query("DELETE FROM strunk_snapshots").execute(pool).await.ok();
    sqlx::query("DELETE FROM strunk_results").execute(pool).await.ok();
    sqlx::query("DELETE FROM strunk_schedules").execute(pool).await.ok();
}

#[tokio::test]
async fn heartbeat_extends_visibility() {
    let Some(strunk) = setup().await else { return };

    let mut tx = strunk.begin().await.unwrap();
    strunk.task(&mut tx, "hb-queue")
        .payload(serde_json::json!({"long_running": true}))
        .submit().await.unwrap();
    tx.commit().await.unwrap();

    let task = strunk.claim("hb-queue", Duration::from_millis(200)).await.unwrap().unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;
    strunk.heartbeat(task.id, Duration::from_secs(5)).await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    let stolen = strunk.claim("hb-queue", Duration::from_secs(30)).await.unwrap();
    assert!(stolen.is_none(), "heartbeat should have extended visibility, task should not be reclaimable");

    strunk.complete(task.id).await.unwrap();
}

#[tokio::test]
async fn progress_tracking() {
    let Some(strunk) = setup().await else { return };

    let mut tx = strunk.begin().await.unwrap();
    strunk.task(&mut tx, "progress-queue")
        .payload(serde_json::json!({"trackable": true}))
        .submit().await.unwrap();
    tx.commit().await.unwrap();

    let task = strunk.claim("progress-queue", Duration::from_secs(30)).await.unwrap().unwrap();

    let p = strunk.get_progress(task.id).await.unwrap().unwrap();
    assert_eq!(p, 0);

    strunk.set_progress(task.id, 50).await.unwrap();
    let p = strunk.get_progress(task.id).await.unwrap().unwrap();
    assert_eq!(p, 50);

    strunk.set_progress(task.id, 100).await.unwrap();
    let p = strunk.get_progress(task.id).await.unwrap().unwrap();
    assert_eq!(p, 100);

    strunk.set_progress(task.id, 150).await.unwrap();
    let p = strunk.get_progress(task.id).await.unwrap().unwrap();
    assert_eq!(p, 100, "progress should be clamped to 100");
}

#[tokio::test]
async fn complete_with_result() {
    let Some(strunk) = setup().await else { return };

    let mut tx = strunk.begin().await.unwrap();
    let task_id = strunk.task(&mut tx, "result-queue")
        .payload(serde_json::json!({"compute": "something"}))
        .submit().await.unwrap();
    tx.commit().await.unwrap();

    let task = strunk.claim("result-queue", Duration::from_secs(30)).await.unwrap().unwrap();

    strunk.complete_with_result(
        task.id,
        "result-queue",
        serde_json::json!({"answer": 42, "status": "success"}),
    ).await.unwrap();

    let result = strunk.get_result(task_id).await.unwrap().unwrap();
    assert_eq!(result.task_id, task_id);
    assert_eq!(result.queue, "result-queue");
    assert_eq!(result.result["answer"], 42);
}

#[tokio::test]
async fn get_result_missing_returns_none() {
    let Some(strunk) = setup().await else { return };

    let result = strunk.get_result(999999).await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn task_exposes_max_retries() {
    let Some(strunk) = setup().await else { return };

    let mut tx = strunk.begin().await.unwrap();
    strunk.task(&mut tx, "retries-queue")
        .payload(serde_json::json!({}))
        .max_retries(7)
        .submit().await.unwrap();
    tx.commit().await.unwrap();

    let task = strunk.claim("retries-queue", Duration::from_secs(30)).await.unwrap().unwrap();
    assert_eq!(task.max_retries, 7);
}

#[tokio::test]
async fn schedule_register_and_list() {
    let Some(strunk) = setup().await else { return };

    strunk.schedule("daily-report", "reports", "every 1d")
        .payload(serde_json::json!({"type": "daily"}))
        .priority(3)
        .register().await.unwrap();

    strunk.schedule("hourly-sync", "sync", "@hourly")
        .register().await.unwrap();

    let schedules = strunk.list_schedules().await.unwrap();
    assert_eq!(schedules.len(), 2);

    let daily = schedules.iter().find(|s| s.id == "daily-report").unwrap();
    assert_eq!(daily.queue, "reports");
    assert_eq!(daily.priority, 3);
    assert!(daily.enabled);
}

#[tokio::test]
async fn schedule_disable_and_enable() {
    let Some(strunk) = setup().await else { return };

    strunk.schedule("toggle-sched", "queue", "every 1h")
        .register().await.unwrap();

    strunk.disable_schedule("toggle-sched").await.unwrap();
    let schedules = strunk.list_schedules().await.unwrap();
    assert!(!schedules[0].enabled);

    strunk.enable_schedule("toggle-sched").await.unwrap();
    let schedules = strunk.list_schedules().await.unwrap();
    assert!(schedules[0].enabled);
}

#[tokio::test]
async fn schedule_remove() {
    let Some(strunk) = setup().await else { return };

    strunk.schedule("temp-sched", "queue", "every 5m")
        .register().await.unwrap();

    strunk.remove_schedule("temp-sched").await.unwrap();
    let schedules = strunk.list_schedules().await.unwrap();
    assert!(schedules.is_empty());
}

#[tokio::test]
async fn scheduler_fires_due_tasks() {
    let Some(strunk) = setup().await else { return };

    strunk.schedule("fire-test", "sched-queue", "every 30s")
        .payload(serde_json::json!({"scheduled": true}))
        .register().await.unwrap();

    sqlx::query("UPDATE strunk_schedules SET next_fire = now() - interval '1 second' WHERE id = 'fire-test'")
        .execute(strunk.pool())
        .await.unwrap();

    let sched_handle = strunk.scheduler()
        .interval(Duration::from_millis(100))
        .spawn();

    tokio::time::sleep(Duration::from_millis(500)).await;

    strunk.shutdown();
    tokio::time::timeout(Duration::from_secs(2), sched_handle).await.ok();

    let count = strunk.pending_count("sched-queue").await.unwrap();
    assert!(count >= 1, "scheduler should have fired at least one task, got {}", count);

    let task = strunk.claim("sched-queue", Duration::from_secs(30)).await.unwrap().unwrap();
    assert_eq!(task.payload["scheduled"], true);
}

#[tokio::test]
async fn scheduler_does_not_double_fire() {
    let Some(strunk) = setup().await else { return };

    strunk.schedule("dedup-fire", "dedup-sched-queue", "every 30s")
        .register().await.unwrap();

    sqlx::query("UPDATE strunk_schedules SET next_fire = now() - interval '1 second' WHERE id = 'dedup-fire'")
        .execute(strunk.pool())
        .await.unwrap();

    let sched1 = strunk.scheduler().interval(Duration::from_millis(100)).spawn();
    let sched2 = strunk.scheduler().interval(Duration::from_millis(100)).spawn();

    tokio::time::sleep(Duration::from_millis(500)).await;

    strunk.shutdown();
    tokio::time::timeout(Duration::from_secs(2), sched1).await.ok();
    tokio::time::timeout(Duration::from_secs(2), sched2).await.ok();

    let count = strunk.pending_count("dedup-sched-queue").await.unwrap();
    assert_eq!(count, 1, "two schedulers should not double-fire, got {}", count);
}

#[tokio::test]
async fn schedule_update_on_re_register() {
    let Some(strunk) = setup().await else { return };

    strunk.schedule("evolving", "queue-v1", "every 1h")
        .payload(serde_json::json!({"version": 1}))
        .register().await.unwrap();

    strunk.schedule("evolving", "queue-v2", "every 30m")
        .payload(serde_json::json!({"version": 2}))
        .priority(5)
        .register().await.unwrap();

    let schedules = strunk.list_schedules().await.unwrap();
    assert_eq!(schedules.len(), 1);
    assert_eq!(schedules[0].queue, "queue-v2");
    assert_eq!(schedules[0].priority, 5);
}

#[tokio::test]
async fn worker_with_result_storage() {
    let Some(strunk) = setup().await else { return };

    let mut tx = strunk.begin().await.unwrap();
    let task_id = strunk.task(&mut tx, "compute-queue")
        .payload(serde_json::json!({"x": 6, "y": 7}))
        .submit().await.unwrap();
    tx.commit().await.unwrap();

    let completed = Arc::new(AtomicI64::new(0));
    let completed_clone = completed.clone();
    let pool = strunk.pool().clone();

    let handles = strunk.worker("compute-queue")
        .concurrency(1)
        .poll_interval(Duration::from_millis(50))
        .spawn(move |task| {
            let pool = pool.clone();
            let completed = completed_clone.clone();
            async move {
                let x = task.payload["x"].as_i64().unwrap();
                let y = task.payload["y"].as_i64().unwrap();
                let result = serde_json::json!({"product": x * y});

                strunk::task_queue::complete_with_result(&pool, task.id, &task.queue, result).await
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                completed.fetch_add(1, Ordering::Relaxed);
                Err("skip default complete".into())
            }
        });

    let start = std::time::Instant::now();
    loop {
        if completed.load(Ordering::Relaxed) >= 1 { break; }
        tokio::time::sleep(Duration::from_millis(50)).await;
        if start.elapsed() > Duration::from_secs(5) {
            panic!("worker timed out");
        }
    }

    strunk.shutdown();
    for h in handles { tokio::time::timeout(Duration::from_secs(2), h).await.ok(); }

    let result = strunk.get_result(task_id).await.unwrap().unwrap();
    assert_eq!(result.result["product"], 42);
}

#[tokio::test]
async fn heartbeat_in_worker_prevents_timeout() {
    let Some(strunk) = setup().await else { return };

    let mut tx = strunk.begin().await.unwrap();
    strunk.task(&mut tx, "slow-queue")
        .payload(serde_json::json!({"slow": true}))
        .submit().await.unwrap();
    tx.commit().await.unwrap();

    let pool = strunk.pool().clone();
    let completed = Arc::new(AtomicI64::new(0));
    let completed_clone = completed.clone();

    let handles = strunk.worker("slow-queue")
        .concurrency(1)
        .visibility_timeout(Duration::from_millis(300))
        .poll_interval(Duration::from_millis(50))
        .spawn(move |task| {
            let pool = pool.clone();
            let completed = completed_clone.clone();
            async move {
                for _ in 0..5 {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    strunk::task_queue::heartbeat(&pool, task.id, Duration::from_millis(500)).await
                        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                }
                completed.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
        });

    let start = std::time::Instant::now();
    loop {
        if completed.load(Ordering::Relaxed) >= 1 { break; }
        tokio::time::sleep(Duration::from_millis(50)).await;
        if start.elapsed() > Duration::from_secs(10) {
            panic!("worker with heartbeat timed out");
        }
    }

    strunk.shutdown();
    for h in handles { tokio::time::timeout(Duration::from_secs(2), h).await.ok(); }

    let pending = strunk.pending_count("slow-queue").await.unwrap();
    assert_eq!(pending, 0, "task should have completed successfully with heartbeats");
}

#[tokio::test]
async fn cron_expression_formats() {
    let Some(strunk) = setup().await else { return };

    strunk.schedule("s1", "q", "every 30s").register().await.unwrap();
    strunk.schedule("s2", "q", "every 5m").register().await.unwrap();
    strunk.schedule("s3", "q", "every 2h").register().await.unwrap();
    strunk.schedule("s4", "q", "every 1d").register().await.unwrap();
    strunk.schedule("s5", "q", "@hourly").register().await.unwrap();
    strunk.schedule("s6", "q", "@daily").register().await.unwrap();
    strunk.schedule("s7", "q", "@weekly").register().await.unwrap();
    strunk.schedule("s8", "q", "30s").register().await.unwrap();
    strunk.schedule("s9", "q", "5m").register().await.unwrap();

    let schedules = strunk.list_schedules().await.unwrap();
    assert_eq!(schedules.len(), 9, "all schedule formats should be accepted");
}

#[tokio::test]
async fn invalid_cron_rejected() {
    let Some(strunk) = setup().await else { return };

    let result = strunk.schedule("bad", "q", "").register().await;
    assert!(result.is_err());
}
