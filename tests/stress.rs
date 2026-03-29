use std::collections::HashSet;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use sqlx::PgPool;
use strunk::config::StrunkConfig;
use strunk::{BatchItem, Strunk};
use tokio::sync::Mutex;

fn test_config() -> Option<StrunkConfig> {
    let url = std::env::var("DATABASE_URL").ok()?;
    Some(StrunkConfig {
        database_url: url,
        poll_interval: Duration::from_millis(10),
        relay_batch_size: 500,
        reaper_retention_delivered: Duration::from_secs(1),
        reaper_retention_dead: Duration::from_secs(1),
        reaper_batch_size: 1000,
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
}

#[tokio::test]
async fn stress_concurrent_submit_1000_tasks() {
    let Some(strunk) = setup().await else { return };

    let start = Instant::now();
    let mut handles = vec![];

    for i in 0..10 {
        let pool = strunk.pool().clone();
        handles.push(tokio::spawn(async move {
            let items: Vec<BatchItem> = (0..100)
                .map(|j| BatchItem::new("stress-queue", serde_json::json!({"batch": i, "item": j})))
                .collect();
            let mut tx = pool.begin().await.unwrap();
            strunk::task_queue::submit_batch(&mut tx, items).await.unwrap();
            tx.commit().await.unwrap();
        }));
    }

    for h in handles { h.await.unwrap(); }

    let elapsed = start.elapsed();
    let count = strunk.pending_count("stress-queue").await.unwrap();
    assert_eq!(count, 1000, "all 1000 tasks should be pending");
    eprintln!("submitted 1000 tasks in {:?}", elapsed);
}

#[tokio::test]
async fn stress_concurrent_claim_no_duplicates() {
    let Some(strunk) = setup().await else { return };

    let task_count = 200;
    let worker_count = 20;

    let items: Vec<BatchItem> = (0..task_count)
        .map(|i| BatchItem::new("nodupe-queue", serde_json::json!({"i": i})))
        .collect();
    let mut tx = strunk.begin().await.unwrap();
    strunk.submit_batch(&mut tx, items).await.unwrap();
    tx.commit().await.unwrap();

    let claimed_ids = Arc::new(Mutex::new(Vec::new()));
    let mut handles = vec![];

    for _ in 0..worker_count {
        let pool = strunk.pool().clone();
        let ids = claimed_ids.clone();
        handles.push(tokio::spawn(async move {
            loop {
                match strunk::task_queue::claim(&pool, "nodupe-queue", Duration::from_secs(30)).await.unwrap() {
                    Some(task) => {
                        ids.lock().await.push(task.id);
                        strunk::task_queue::complete(&pool, task.id).await.unwrap();
                    }
                    None => break,
                }
            }
        }));
    }

    for h in handles { h.await.unwrap(); }

    let ids = claimed_ids.lock().await;
    let unique: HashSet<_> = ids.iter().collect();
    assert_eq!(ids.len(), task_count, "should have claimed all {} tasks", task_count);
    assert_eq!(unique.len(), task_count, "no duplicates allowed");
}

#[tokio::test]
async fn stress_worker_throughput() {
    let Some(strunk) = setup().await else { return };

    let task_count = 500;
    let items: Vec<BatchItem> = (0..task_count)
        .map(|i| BatchItem::new("throughput-queue", serde_json::json!({"i": i})))
        .collect();
    let mut tx = strunk.begin().await.unwrap();
    strunk.submit_batch(&mut tx, items).await.unwrap();
    tx.commit().await.unwrap();

    let completed = Arc::new(AtomicI64::new(0));
    let completed_clone = completed.clone();

    let start = Instant::now();
    let handles = strunk.worker("throughput-queue")
        .concurrency(8)
        .poll_interval(Duration::from_millis(10))
        .spawn(move |_task| {
            let completed = completed_clone.clone();
            async move {
                completed.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
        });

    loop {
        let done = completed.load(Ordering::Relaxed);
        if done >= task_count as i64 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        if start.elapsed() > Duration::from_secs(30) {
            panic!("timed out after 30s with {} of {} tasks completed", done, task_count);
        }
    }

    let elapsed = start.elapsed();
    let throughput = task_count as f64 / elapsed.as_secs_f64();
    eprintln!("processed {} tasks in {:?} ({:.0} tasks/sec)", task_count, elapsed, throughput);

    strunk.shutdown();
    for h in handles { tokio::time::timeout(Duration::from_secs(5), h).await.ok(); }

    let pending = strunk.pending_count("throughput-queue").await.unwrap();
    assert_eq!(pending, 0, "all tasks should be processed");
}

#[tokio::test]
async fn stress_large_batch_insert() {
    let Some(strunk) = setup().await else { return };

    let batch_size = 5000;
    let items: Vec<BatchItem> = (0..batch_size)
        .map(|i| BatchItem::new("bigbatch-queue", serde_json::json!({"i": i})).priority(i as i32 % 10))
        .collect();

    let start = Instant::now();
    let mut tx = strunk.begin().await.unwrap();
    let ids = strunk.submit_batch(&mut tx, items).await.unwrap();
    tx.commit().await.unwrap();
    let elapsed = start.elapsed();

    assert_eq!(ids.len(), batch_size);
    eprintln!("batch inserted {} tasks in {:?}", batch_size, elapsed);

    let first = strunk.claim("bigbatch-queue", Duration::from_secs(30)).await.unwrap().unwrap();
    assert_eq!(first.payload["i"].as_i64().unwrap() % 10, 9, "highest priority should come first");
}

#[tokio::test]
async fn stress_rapid_publish_subscribe() {
    let Some(mut strunk) = setup().await else { return };

    strunk.register_schema("tick", "1.0", &serde_json::json!({
        "properties": { "seq": {"type": "integer"} },
        "required": ["seq"]
    })).unwrap();

    let publish_count = 500;
    for i in 0..publish_count {
        let mut tx = strunk.begin().await.unwrap();
        strunk.event(&mut tx, "tick", &(i % 10).to_string())
            .state(serde_json::json!({"seq": i}))
            .schema_version("1.0")
            .publish().await.unwrap();
        tx.commit().await.unwrap();
    }

    let received = Arc::new(AtomicI64::new(0));
    let received_clone = received.clone();

    let sub = strunk.subscriber("rapid-sub", "tick")
        .poll_interval(Duration::from_millis(10))
        .batch_size(100)
        .spawn(move |_event| {
            let received = received_clone.clone();
            async move {
                received.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
        });

    let start = Instant::now();
    loop {
        let count = received.load(Ordering::Relaxed);
        if count >= publish_count {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        if start.elapsed() > Duration::from_secs(30) {
            panic!("subscriber timed out with {} of {} received", count, publish_count);
        }
    }

    let elapsed = start.elapsed();
    eprintln!("subscriber processed {} changes in {:?}", publish_count, elapsed);
    sub.abort();
}

#[tokio::test]
async fn stress_stats_under_load() {
    let Some(strunk) = setup().await else { return };

    let items: Vec<BatchItem> = (0..100)
        .map(|i| BatchItem::new("stats-load-queue", serde_json::json!({"i": i})))
        .collect();
    let mut tx = strunk.begin().await.unwrap();
    strunk.submit_batch(&mut tx, items).await.unwrap();
    tx.commit().await.unwrap();

    let mut handles = vec![];
    for _ in 0..5 {
        let pool = strunk.pool().clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..10 {
                if let Some(task) = strunk::task_queue::claim(&pool, "stats-load-queue", Duration::from_secs(30)).await.unwrap() {
                    strunk::task_queue::complete(&pool, task.id).await.unwrap();
                }
            }
        }));
    }

    for _ in 0..20 {
        let stats = strunk.queue_stats("stats-load-queue").await.unwrap();
        assert!(stats.pending + stats.claimed + stats.delivered <= 100);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    for h in handles { h.await.unwrap(); }

    let overall = strunk.overall_stats().await.unwrap();
    assert!(overall.total_delivered > 0);
}
