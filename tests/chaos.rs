use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;
use strunk::config::StrunkConfig;
use strunk::{BatchItem, Strunk};
use tokio::sync::Mutex;

fn test_config() -> Option<StrunkConfig> {
    let url = std::env::var("DATABASE_URL").ok()?;
    Some(StrunkConfig {
        database_url: url,
        poll_interval: Duration::from_millis(10),
        relay_batch_size: 100,
        reaper_retention_delivered: Duration::from_secs(1),
        reaper_retention_dead: Duration::from_secs(1),
        reaper_batch_size: 100,
        reaper_interval: Duration::from_millis(500),
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
}

#[tokio::test]
async fn chaos_worker_crash_task_recovers() {
    let Some(strunk) = setup().await else { return };

    let mut tx = strunk.begin().await.unwrap();
    strunk.task(&mut tx, "crash-queue")
        .payload(serde_json::json!({"recoverable": true}))
        .submit().await.unwrap();
    tx.commit().await.unwrap();

    let task = strunk.claim("crash-queue", Duration::from_millis(200)).await.unwrap().unwrap();
    let task_id = task.id;

    drop(task);

    let none = strunk.claim("crash-queue", Duration::from_secs(30)).await.unwrap();
    assert!(none.is_none(), "should not be claimable while visibility timeout is active");

    tokio::time::sleep(Duration::from_millis(300)).await;

    let recovered = strunk.claim("crash-queue", Duration::from_secs(30)).await.unwrap().unwrap();
    assert_eq!(recovered.id, task_id, "same task should reappear after crash");
    assert_eq!(recovered.attempts, 2, "attempt count should have incremented");
}

#[tokio::test]
async fn chaos_poison_message_goes_to_dead_letter() {
    let Some(strunk) = setup().await else { return };

    let mut tx = strunk.begin().await.unwrap();
    strunk.task(&mut tx, "poison-queue")
        .payload(serde_json::json!({"poison": true}))
        .max_retries(3)
        .submit().await.unwrap();
    tx.commit().await.unwrap();

    let fail_count = Arc::new(AtomicI64::new(0));
    let fail_count_clone = fail_count.clone();

    let handles = strunk.worker("poison-queue")
        .concurrency(1)
        .visibility_timeout(Duration::from_millis(100))
        .poll_interval(Duration::from_millis(50))
        .spawn(move |_task| {
            let count = fail_count_clone.clone();
            async move {
                count.fetch_add(1, Ordering::Relaxed);
                Err("always fails".into())
            }
        });

    tokio::time::sleep(Duration::from_secs(5)).await;
    strunk.shutdown();
    for h in handles { tokio::time::timeout(Duration::from_secs(2), h).await.ok(); }

    let dead = strunk.dead_letters("poison-queue", 10).await.unwrap();
    assert_eq!(dead.len(), 1, "poison message should end up in dead letter");

    let failures = fail_count.load(Ordering::Relaxed);
    assert!(failures >= 3, "handler should have been called at least 3 times (max_retries), got {}", failures);
}

#[tokio::test]
async fn chaos_rapid_shutdown_during_processing() {
    let Some(strunk) = setup().await else { return };

    let items: Vec<BatchItem> = (0..100)
        .map(|i| BatchItem::new("shutdown-queue", serde_json::json!({"i": i})))
        .collect();
    let mut tx = strunk.begin().await.unwrap();
    strunk.submit_batch(&mut tx, items).await.unwrap();
    tx.commit().await.unwrap();

    let completed = Arc::new(AtomicI64::new(0));
    let completed_clone = completed.clone();

    let handles = strunk.worker("shutdown-queue")
        .concurrency(4)
        .poll_interval(Duration::from_millis(10))
        .spawn(move |_task| {
            let completed = completed_clone.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(10)).await;
                completed.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
        });

    tokio::time::sleep(Duration::from_millis(100)).await;
    strunk.shutdown();

    for h in handles {
        let result = tokio::time::timeout(Duration::from_secs(5), h).await;
        assert!(result.is_ok(), "worker should shut down within 5s");
    }

    let done = completed.load(Ordering::Relaxed);
    let pending = strunk.pending_count("shutdown-queue").await.unwrap();
    eprintln!("completed {} tasks before shutdown, {} still pending", done, pending);

    assert!(done > 0, "should have completed some tasks before shutdown");
    assert_eq!(done + pending, 100, "no tasks should be lost (completed + pending = 100), got {} + {}", done, pending);
}

#[tokio::test]
async fn chaos_concurrent_reaper_and_workers() {
    let Some(strunk) = setup().await else { return };

    let items: Vec<BatchItem> = (0..200)
        .map(|i| BatchItem::new("reaper-race-queue", serde_json::json!({"i": i})))
        .collect();
    let mut tx = strunk.begin().await.unwrap();
    strunk.submit_batch(&mut tx, items).await.unwrap();
    tx.commit().await.unwrap();

    let reaper = strunk.reaper()
        .retention_delivered(Duration::from_millis(100))
        .retention_dead(Duration::from_millis(100))
        .interval(Duration::from_millis(200))
        .spawn();

    let completed = Arc::new(AtomicI64::new(0));
    let completed_clone = completed.clone();

    let workers = strunk.worker("reaper-race-queue")
        .concurrency(4)
        .poll_interval(Duration::from_millis(10))
        .spawn(move |_task| {
            let completed = completed_clone.clone();
            async move {
                completed.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
        });

    let start = std::time::Instant::now();
    loop {
        if completed.load(Ordering::Relaxed) >= 200 { break; }
        tokio::time::sleep(Duration::from_millis(50)).await;
        if start.elapsed() > Duration::from_secs(30) {
            panic!("timed out waiting for workers with reaper running");
        }
    }

    strunk.shutdown();
    reaper.abort();
    for h in workers { tokio::time::timeout(Duration::from_secs(2), h).await.ok(); }

    assert_eq!(completed.load(Ordering::Relaxed), 200);
}

#[tokio::test]
async fn chaos_subscriber_crash_and_resume() {
    let Some(mut strunk) = setup().await else { return };

    strunk.register_schema("event", "1.0", &serde_json::json!({
        "properties": { "seq": {"type": "integer"} },
        "required": ["seq"]
    })).unwrap();

    for i in 0..20 {
        let mut tx = strunk.begin().await.unwrap();
        strunk.change(&mut tx, "event", &i.to_string())
            .state(serde_json::json!({"seq": i}))
            .schema_version("1.0")
            .publish().await.unwrap();
        tx.commit().await.unwrap();
    }

    let crash_at = 7;
    let seen_before_crash = Arc::new(Mutex::new(Vec::new()));
    let seen_clone = seen_before_crash.clone();

    let sub = strunk.subscriber("crash-sub", "event")
        .poll_interval(Duration::from_millis(50))
        .spawn(move |change| {
            let seen = seen_clone.clone();
            async move {
                let seq = change.state["seq"].as_i64().unwrap();
                if seq == crash_at {
                    return Err("simulated crash".into());
                }
                seen.lock().await.push(seq);
                Ok(())
            }
        });

    tokio::time::sleep(Duration::from_millis(500)).await;
    sub.abort();

    let before = seen_before_crash.lock().await.clone();
    assert_eq!(before.len(), crash_at as usize, "should have processed {} before crash, got {}", crash_at, before.len());

    let seen_after = Arc::new(Mutex::new(Vec::new()));
    let seen_clone = seen_after.clone();

    let sub2 = strunk.subscriber("crash-sub", "event")
        .poll_interval(Duration::from_millis(50))
        .spawn(move |change| {
            let seen = seen_clone.clone();
            async move {
                seen.lock().await.push(change.state["seq"].as_i64().unwrap());
                Ok(())
            }
        });

    tokio::time::sleep(Duration::from_millis(1000)).await;
    sub2.abort();

    let after = seen_after.lock().await.clone();
    assert!(!after.is_empty(), "resumed subscriber should process remaining changes");
    assert_eq!(*after.first().unwrap(), crash_at, "should resume from the crash point (seq {}), got {}", crash_at, after.first().unwrap());
}

#[tokio::test]
async fn chaos_transaction_rollback_under_load() {
    let Some(strunk) = setup().await else { return };

    let mut successful = 0i64;
    for i in 0..50 {
        let mut tx = strunk.begin().await.unwrap();
        strunk.task(&mut tx, "rollback-queue")
            .payload(serde_json::json!({"i": i}))
            .submit().await.unwrap();

        if i % 3 == 0 {
            drop(tx);
        } else {
            tx.commit().await.unwrap();
            successful += 1;
        }
    }

    let count = strunk.pending_count("rollback-queue").await.unwrap();
    assert_eq!(count, successful, "only committed tasks should be visible");
}

#[tokio::test]
async fn chaos_many_queues_isolation() {
    let Some(strunk) = setup().await else { return };

    let queue_count = 20;
    let tasks_per_queue = 10;

    let mut tx = strunk.begin().await.unwrap();
    for q in 0..queue_count {
        for t in 0..tasks_per_queue {
            strunk.task(&mut tx, &format!("iso-queue-{}", q))
                .payload(serde_json::json!({"queue": q, "task": t}))
                .submit().await.unwrap();
        }
    }
    tx.commit().await.unwrap();

    let mut handles = vec![];
    for q in 0..queue_count {
        let pool = strunk.pool().clone();
        let queue_name = format!("iso-queue-{}", q);
        handles.push(tokio::spawn(async move {
            let mut count = 0;
            loop {
                match strunk::task_queue::claim(&pool, &queue_name, Duration::from_secs(30)).await.unwrap() {
                    Some(task) => {
                        assert_eq!(task.payload["queue"], q, "got task from wrong queue");
                        strunk::task_queue::complete(&pool, task.id).await.unwrap();
                        count += 1;
                    }
                    None => break,
                }
            }
            count
        }));
    }

    let mut total = 0;
    for h in handles {
        let count: i32 = h.await.unwrap();
        assert_eq!(count, tasks_per_queue, "each queue should have exactly {} tasks", tasks_per_queue);
        total += count;
    }
    assert_eq!(total, queue_count * tasks_per_queue);
}

#[tokio::test]
async fn chaos_health_degrades_with_stale_tasks() {
    let Some(strunk) = setup().await else { return };

    let report = strunk.health(Duration::from_secs(1)).await.unwrap();
    assert!(report.healthy, "empty system should be healthy");

    let mut tx = strunk.begin().await.unwrap();
    strunk.task(&mut tx, "stale-queue")
        .payload(serde_json::json!({"will_get_stale": true}))
        .submit().await.unwrap();
    tx.commit().await.unwrap();

    sqlx::query("UPDATE strunk_outbox SET created_at = now() - interval '10 minutes' WHERE key = 'stale-queue'")
        .execute(strunk.pool())
        .await.unwrap();

    let report = strunk.health(Duration::from_secs(300)).await.unwrap();
    assert!(!report.healthy, "system should be unhealthy with 10-minute-old pending task and 5-minute threshold");
    assert!(report.oldest_pending_age_secs.unwrap() >= 500);
}

#[tokio::test]
async fn chaos_reaper_under_high_delivery_rate() {
    let Some(strunk) = setup().await else { return };

    let items: Vec<BatchItem> = (0..500)
        .map(|i| BatchItem::new("reaper-load-queue", serde_json::json!({"i": i})))
        .collect();
    let mut tx = strunk.begin().await.unwrap();
    strunk.submit_batch(&mut tx, items).await.unwrap();
    tx.commit().await.unwrap();

    let completed = Arc::new(AtomicI64::new(0));
    let completed_clone = completed.clone();

    let workers = strunk.worker("reaper-load-queue")
        .concurrency(4)
        .poll_interval(Duration::from_millis(10))
        .spawn(move |_task| {
            let completed = completed_clone.clone();
            async move {
                completed.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
        });

    let reaper = strunk.reaper()
        .retention_delivered(Duration::from_millis(50))
        .retention_dead(Duration::from_millis(50))
        .interval(Duration::from_millis(100))
        .spawn();

    let start = std::time::Instant::now();
    loop {
        if completed.load(Ordering::Relaxed) >= 500 { break; }
        tokio::time::sleep(Duration::from_millis(50)).await;
        if start.elapsed() > Duration::from_secs(30) {
            panic!("timed out");
        }
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    strunk.shutdown();
    reaper.abort();
    for h in workers { tokio::time::timeout(Duration::from_secs(2), h).await.ok(); }

    let overall = strunk.overall_stats().await.unwrap();
    eprintln!("after reaper: {} delivered, {} total rows", overall.total_delivered, overall.table_size);
    assert!(overall.table_size < 500, "reaper should have cleaned up some delivered rows, got {}", overall.table_size);
}
