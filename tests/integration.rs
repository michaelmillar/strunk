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
    sqlx::query("DELETE FROM strunk_outbox")
        .execute(pool)
        .await
        .ok();
    sqlx::query("DELETE FROM strunk_subscribers")
        .execute(pool)
        .await
        .ok();
    sqlx::query("DELETE FROM strunk_snapshots")
        .execute(pool)
        .await
        .ok();
}

#[tokio::test]
async fn task_submit_and_claim() {
    let Some(strunk) = setup().await else {
        eprintln!("skipping: DATABASE_URL not set");
        return;
    };

    let mut tx = strunk.begin().await.unwrap();
    let id = strunk
        .task(&mut tx, "test-queue")
        .payload(serde_json::json!({"action": "send_email"}))
        .priority(5)
        .max_retries(2)
        .submit()
        .await
        .unwrap();
    tx.commit().await.unwrap();

    assert!(id > 0);

    let task = strunk
        .claim("test-queue", Duration::from_secs(30))
        .await
        .unwrap()
        .expect("should have a task to claim");

    assert_eq!(task.id, id);
    assert_eq!(task.queue, "test-queue");
    assert_eq!(task.payload["action"], "send_email");
    assert_eq!(task.attempts, 1);

    strunk.complete(task.id).await.unwrap();

    let next = strunk
        .claim("test-queue", Duration::from_secs(30))
        .await
        .unwrap();
    assert!(next.is_none(), "queue should be empty after complete");
}

#[tokio::test]
async fn task_fail_and_dead_letter() {
    let Some(strunk) = setup().await else {
        eprintln!("skipping: DATABASE_URL not set");
        return;
    };

    let mut tx = strunk.begin().await.unwrap();
    strunk
        .task(&mut tx, "fail-queue")
        .payload(serde_json::json!({"bad": true}))
        .max_retries(1)
        .submit()
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let task = strunk
        .claim("fail-queue", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();

    strunk.fail(task.id, 1, task.attempts).await.unwrap();

    let dead = strunk.dead_letters("fail-queue", 10).await.unwrap();
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].id, task.id);

    strunk.retry_dead(task.id).await.unwrap();

    let retried = strunk
        .claim("fail-queue", Duration::from_secs(30))
        .await
        .unwrap();
    assert!(retried.is_some(), "dead-lettered task should be reclaimable after retry");
}

#[tokio::test]
async fn task_priority_ordering() {
    let Some(strunk) = setup().await else {
        eprintln!("skipping: DATABASE_URL not set");
        return;
    };

    let mut tx = strunk.begin().await.unwrap();
    strunk
        .task(&mut tx, "prio-queue")
        .payload(serde_json::json!({"name": "low"}))
        .priority(1)
        .submit()
        .await
        .unwrap();
    strunk
        .task(&mut tx, "prio-queue")
        .payload(serde_json::json!({"name": "high"}))
        .priority(10)
        .submit()
        .await
        .unwrap();
    strunk
        .task(&mut tx, "prio-queue")
        .payload(serde_json::json!({"name": "medium"}))
        .priority(5)
        .submit()
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let first = strunk
        .claim("prio-queue", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.payload["name"], "high");
    strunk.complete(first.id).await.unwrap();

    let second = strunk
        .claim("prio-queue", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second.payload["name"], "medium");
    strunk.complete(second.id).await.unwrap();

    let third = strunk
        .claim("prio-queue", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(third.payload["name"], "low");
}

#[tokio::test]
async fn event_publish_and_snapshot() {
    let Some(mut strunk) = setup().await else {
        eprintln!("skipping: DATABASE_URL not set");
        return;
    };

    strunk
        .register_schema(
            "order",
            "1.0",
            &serde_json::json!({
                "properties": {
                    "id": { "type": "integer" },
                    "status": { "type": "string" },
                    "total": { "type": "number" }
                },
                "required": ["id", "status", "total"]
            }),
        )
        .unwrap();

    let mut tx = strunk.begin().await.unwrap();
    let event_id = strunk
        .event(&mut tx, "order", "42")
        .state(serde_json::json!({
            "id": 42,
            "status": "confirmed",
            "total": 59.99
        }))
        .schema_version("1.0")
        .publish()
        .await
        .unwrap();
    tx.commit().await.unwrap();

    assert!(event_id > 0);

    let state = strunk.snapshot("order", "42").await.unwrap();
    assert!(state.is_some());
    let state = state.unwrap();
    assert_eq!(state["status"], "confirmed");
    assert_eq!(state["total"], 59.99);
}

#[tokio::test]
async fn event_snapshot_updates() {
    let Some(mut strunk) = setup().await else {
        eprintln!("skipping: DATABASE_URL not set");
        return;
    };

    strunk
        .register_schema(
            "order",
            "1.0",
            &serde_json::json!({
                "properties": {
                    "id": { "type": "integer" },
                    "status": { "type": "string" },
                    "total": { "type": "number" }
                },
                "required": ["id", "status", "total"]
            }),
        )
        .unwrap();

    let mut tx = strunk.begin().await.unwrap();
    strunk
        .event(&mut tx, "order", "99")
        .state(serde_json::json!({
            "id": 99,
            "status": "pending",
            "total": 10.0
        }))
        .schema_version("1.0")
        .publish()
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let mut tx = strunk.begin().await.unwrap();
    strunk
        .event(&mut tx, "order", "99")
        .state(serde_json::json!({
            "id": 99,
            "status": "shipped",
            "total": 10.0
        }))
        .schema_version("1.0")
        .publish()
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let state = strunk.snapshot("order", "99").await.unwrap().unwrap();
    assert_eq!(state["status"], "shipped");
}

#[tokio::test]
async fn schema_validation_rejects_bad_payload() {
    let Some(mut strunk) = setup().await else {
        eprintln!("skipping: DATABASE_URL not set");
        return;
    };

    strunk
        .register_schema(
            "user",
            "1.0",
            &serde_json::json!({
                "properties": {
                    "id": { "type": "integer" },
                    "name": { "type": "string" }
                },
                "required": ["id", "name"]
            }),
        )
        .unwrap();

    let mut tx = strunk.begin().await.unwrap();
    let result = strunk
        .event(&mut tx, "user", "1")
        .state(serde_json::json!({"id": 1}))
        .schema_version("1.0")
        .publish()
        .await;

    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("name"), "error should mention missing field 'name'");
}

#[tokio::test]
async fn schema_backward_compatibility() {
    let Some(mut strunk) = setup().await else {
        eprintln!("skipping: DATABASE_URL not set");
        return;
    };

    strunk
        .register_schema(
            "product",
            "1.0",
            &serde_json::json!({
                "properties": {
                    "id": { "type": "integer" },
                    "name": { "type": "string" }
                },
                "required": ["id", "name"]
            }),
        )
        .unwrap();

    let compatible = strunk.register_schema(
        "product",
        "1.1",
        &serde_json::json!({
            "properties": {
                "id": { "type": "integer" },
                "name": { "type": "string" },
                "description": { "type": "string" }
            },
            "required": ["id", "name"]
        }),
    );
    assert!(compatible.is_ok(), "adding optional field should be compatible");

    let incompatible = strunk.register_schema(
        "product",
        "2.0",
        &serde_json::json!({
            "properties": {
                "id": { "type": "string" },
                "name": { "type": "string" }
            },
            "required": ["id", "name"]
        }),
    );
    assert!(incompatible.is_err(), "changing field type should be incompatible");
}

#[tokio::test]
async fn worker_processes_tasks() {
    let Some(strunk) = setup().await else {
        eprintln!("skipping: DATABASE_URL not set");
        return;
    };

    let mut tx = strunk.begin().await.unwrap();
    strunk
        .task(&mut tx, "worker-queue")
        .payload(serde_json::json!({"value": 42}))
        .submit()
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let (sender, receiver) = tokio::sync::oneshot::channel::<serde_json::Value>();
    let sender = std::sync::Arc::new(tokio::sync::Mutex::new(Some(sender)));

    let handles = strunk
        .worker("worker-queue")
        .concurrency(1)
        .poll_interval(Duration::from_millis(50))
        .spawn(move |task| {
            let sender = sender.clone();
            async move {
                if let Some(s) = sender.lock().await.take() {
                    s.send(task.payload).ok();
                }
                Ok(())
            }
        });

    let payload = tokio::time::timeout(Duration::from_secs(5), receiver)
        .await
        .expect("worker should process within 5s")
        .expect("channel should not be dropped");

    assert_eq!(payload["value"], 42);

    strunk.shutdown();
    for h in handles {
        tokio::time::timeout(Duration::from_secs(2), h)
            .await
            .expect("worker should shut down within 2s")
            .ok();
    }
}

#[tokio::test]
async fn stats_reflect_queue_state() {
    let Some(strunk) = setup().await else {
        eprintln!("skipping: DATABASE_URL not set");
        return;
    };

    let mut tx = strunk.begin().await.unwrap();
    for i in 0..5 {
        strunk
            .task(&mut tx, "stats-queue")
            .payload(serde_json::json!({"i": i}))
            .submit()
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();

    let stats = strunk.queue_stats("stats-queue").await.unwrap();
    assert_eq!(stats.pending, 5);
    assert_eq!(stats.claimed, 0);
    assert_eq!(stats.dead, 0);

    let task = strunk
        .claim("stats-queue", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    strunk.complete(task.id).await.unwrap();

    let stats = strunk.queue_stats("stats-queue").await.unwrap();
    assert_eq!(stats.pending, 4);
    assert_eq!(stats.delivered, 1);

    let overall = strunk.overall_stats().await.unwrap();
    assert!(overall.total_pending >= 4);
}

#[tokio::test]
async fn transaction_atomicity() {
    let Some(strunk) = setup().await else {
        eprintln!("skipping: DATABASE_URL not set");
        return;
    };

    let mut tx = strunk.begin().await.unwrap();
    strunk
        .task(&mut tx, "atomic-queue")
        .payload(serde_json::json!({"will": "rollback"}))
        .submit()
        .await
        .unwrap();
    drop(tx);

    let task = strunk
        .claim("atomic-queue", Duration::from_secs(30))
        .await
        .unwrap();
    assert!(task.is_none(), "dropped transaction should not leave visible tasks");
}

#[tokio::test]
async fn batch_submit() {
    let Some(strunk) = setup().await else {
        eprintln!("skipping: DATABASE_URL not set");
        return;
    };

    let items = (0..10)
        .map(|i| {
            strunk::BatchItem::new("batch-queue", serde_json::json!({"index": i}))
                .priority(i as i32)
        })
        .collect();

    let mut tx = strunk.begin().await.unwrap();
    let ids = strunk.submit_batch(&mut tx, items).await.unwrap();
    tx.commit().await.unwrap();

    assert_eq!(ids.len(), 10);

    let first = strunk
        .claim("batch-queue", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.payload["index"], 9, "highest priority should come first");
}

#[tokio::test]
async fn health_check() {
    let Some(strunk) = setup().await else {
        eprintln!("skipping: DATABASE_URL not set");
        return;
    };

    let report = strunk.health(Duration::from_secs(300)).await.unwrap();
    assert!(report.healthy);
    assert_eq!(report.pending, 0);
}
