use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;
use strunk::config::StrunkConfig;
use strunk::Strunk;
use tokio::sync::Mutex;

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
}

#[tokio::test]
async fn concurrent_claim_no_double_delivery() {
    let Some(strunk) = setup().await else { return };

    let mut tx = strunk.begin().await.unwrap();
    strunk.task(&mut tx, "race-queue")
        .payload(serde_json::json!({"only_one": true}))
        .submit().await.unwrap();
    tx.commit().await.unwrap();

    let claimed = Arc::new(Mutex::new(Vec::new()));
    let mut handles = vec![];

    for _ in 0..10 {
        let pool = strunk.pool().clone();
        let claimed = claimed.clone();
        handles.push(tokio::spawn(async move {
            let result = strunk::task_queue::claim(&pool, "race-queue", Duration::from_secs(30)).await.unwrap();
            if let Some(task) = result {
                claimed.lock().await.push(task.id);
            }
        }));
    }

    for h in handles { h.await.unwrap(); }

    let claimed = claimed.lock().await;
    assert_eq!(claimed.len(), 1, "exactly one worker should claim the task, got {}", claimed.len());
}

#[tokio::test]
async fn concurrent_claim_all_tasks_distributed() {
    let Some(strunk) = setup().await else { return };

    let mut tx = strunk.begin().await.unwrap();
    for i in 0..20 {
        strunk.task(&mut tx, "dist-queue")
            .payload(serde_json::json!({"i": i}))
            .submit().await.unwrap();
    }
    tx.commit().await.unwrap();

    let claimed_ids = Arc::new(Mutex::new(HashSet::new()));
    let mut handles = vec![];

    for _ in 0..20 {
        let pool = strunk.pool().clone();
        let ids = claimed_ids.clone();
        handles.push(tokio::spawn(async move {
            if let Some(task) = strunk::task_queue::claim(&pool, "dist-queue", Duration::from_secs(30)).await.unwrap() {
                ids.lock().await.insert(task.id);
                strunk::task_queue::complete(&pool, task.id).await.unwrap();
            }
        }));
    }

    for h in handles { h.await.unwrap(); }

    let ids = claimed_ids.lock().await;
    assert_eq!(ids.len(), 20, "all 20 tasks should be claimed by someone, got {}", ids.len());
}

#[tokio::test]
async fn visibility_timeout_makes_task_reclaimable() {
    let Some(strunk) = setup().await else { return };

    let mut tx = strunk.begin().await.unwrap();
    strunk.task(&mut tx, "timeout-queue")
        .payload(serde_json::json!({"timeout_test": true}))
        .submit().await.unwrap();
    tx.commit().await.unwrap();

    let _task = strunk.claim("timeout-queue", Duration::from_millis(100)).await.unwrap().unwrap();

    let none = strunk.claim("timeout-queue", Duration::from_secs(30)).await.unwrap();
    assert!(none.is_none(), "task should not be reclaimable before timeout");

    tokio::time::sleep(Duration::from_millis(200)).await;

    let reclaimed = strunk.claim("timeout-queue", Duration::from_secs(30)).await.unwrap();
    assert!(reclaimed.is_some(), "task should be reclaimable after timeout expires");
}

#[tokio::test]
async fn delayed_task_not_visible_before_time() {
    let Some(strunk) = setup().await else { return };

    let mut tx = strunk.begin().await.unwrap();
    strunk.task(&mut tx, "delay-queue")
        .payload(serde_json::json!({"delayed": true}))
        .delay(Duration::from_secs(2))
        .submit().await.unwrap();
    tx.commit().await.unwrap();

    let none = strunk.claim("delay-queue", Duration::from_secs(30)).await.unwrap();
    assert!(none.is_none(), "delayed task should not be visible yet");

    let count = strunk.pending_count("delay-queue").await.unwrap();
    assert_eq!(count, 1, "task should exist but not be claimable");
}

#[tokio::test]
async fn dedup_key_prevents_duplicate_submission() {
    let Some(strunk) = setup().await else { return };

    let mut tx = strunk.begin().await.unwrap();
    strunk.task(&mut tx, "dedup-queue")
        .payload(serde_json::json!({"order_id": 42}))
        .dedup_key("order-42-email")
        .submit().await.unwrap();
    tx.commit().await.unwrap();

    let mut tx = strunk.begin().await.unwrap();
    let result = strunk.task(&mut tx, "dedup-queue")
        .payload(serde_json::json!({"order_id": 42}))
        .dedup_key("order-42-email")
        .submit().await;
    assert!(result.is_err(), "duplicate dedup_key should be rejected");
}

#[tokio::test]
async fn dedup_key_allows_after_delivery() {
    let Some(strunk) = setup().await else { return };

    let mut tx = strunk.begin().await.unwrap();
    strunk.task(&mut tx, "dedup-reuse-queue")
        .payload(serde_json::json!({"first": true}))
        .dedup_key("reusable-key")
        .submit().await.unwrap();
    tx.commit().await.unwrap();

    let task = strunk.claim("dedup-reuse-queue", Duration::from_secs(30)).await.unwrap().unwrap();
    strunk.complete(task.id).await.unwrap();

    let mut tx = strunk.begin().await.unwrap();
    let result = strunk.task(&mut tx, "dedup-reuse-queue")
        .payload(serde_json::json!({"second": true}))
        .dedup_key("reusable-key")
        .submit().await;
    tx.commit().await.unwrap();

    assert!(result.is_ok(), "same dedup_key should be allowed after previous task is delivered");
}

#[tokio::test]
async fn queue_isolation() {
    let Some(strunk) = setup().await else { return };

    let mut tx = strunk.begin().await.unwrap();
    strunk.task(&mut tx, "queue-a")
        .payload(serde_json::json!({"queue": "a"}))
        .submit().await.unwrap();
    strunk.task(&mut tx, "queue-b")
        .payload(serde_json::json!({"queue": "b"}))
        .submit().await.unwrap();
    tx.commit().await.unwrap();

    let task_a = strunk.claim("queue-a", Duration::from_secs(30)).await.unwrap().unwrap();
    assert_eq!(task_a.payload["queue"], "a");

    let task_b = strunk.claim("queue-b", Duration::from_secs(30)).await.unwrap().unwrap();
    assert_eq!(task_b.payload["queue"], "b");

    let none_a = strunk.claim("queue-a", Duration::from_secs(30)).await.unwrap();
    assert!(none_a.is_none());

    let none_c = strunk.claim("queue-c", Duration::from_secs(30)).await.unwrap();
    assert!(none_c.is_none());
}

#[tokio::test]
async fn empty_queue_returns_none() {
    let Some(strunk) = setup().await else { return };

    let result = strunk.claim("nonexistent-queue", Duration::from_secs(30)).await.unwrap();
    assert!(result.is_none());

    let count = strunk.pending_count("nonexistent-queue").await.unwrap();
    assert_eq!(count, 0);

    let dead = strunk.dead_letters("nonexistent-queue", 10).await.unwrap();
    assert!(dead.is_empty());
}

#[tokio::test]
async fn large_payload() {
    let Some(strunk) = setup().await else { return };

    let big_string = "x".repeat(1_000_000);
    let payload = serde_json::json!({"data": big_string});

    let mut tx = strunk.begin().await.unwrap();
    let id = strunk.task(&mut tx, "big-queue")
        .payload(payload.clone())
        .submit().await.unwrap();
    tx.commit().await.unwrap();

    let task = strunk.claim("big-queue", Duration::from_secs(30)).await.unwrap().unwrap();
    assert_eq!(task.id, id);
    assert_eq!(task.payload["data"].as_str().unwrap().len(), 1_000_000);
}

#[tokio::test]
async fn unicode_in_keys_and_payloads() {
    let Some(strunk) = setup().await else { return };

    let mut tx = strunk.begin().await.unwrap();
    strunk.task(&mut tx, "unicode-queue-\u{1F680}")
        .payload(serde_json::json!({
            "name": "Tschuss\u{00FC}",
            "emoji": "\u{1F389}\u{1F38A}",
            "chinese": "\u{4F60}\u{597D}\u{4E16}\u{754C}",
            "arabic": "\u{0645}\u{0631}\u{062D}\u{0628}\u{0627}"
        }))
        .submit().await.unwrap();
    tx.commit().await.unwrap();

    let task = strunk.claim("unicode-queue-\u{1F680}", Duration::from_secs(30)).await.unwrap().unwrap();
    assert_eq!(task.payload["emoji"], "\u{1F389}\u{1F38A}");
    assert_eq!(task.payload["chinese"], "\u{4F60}\u{597D}\u{4E16}\u{754C}");
}

#[tokio::test]
async fn subscriber_resumes_from_cursor() {
    let Some(mut strunk) = setup().await else { return };

    strunk.register_schema("item", "1.0", &serde_json::json!({
        "properties": { "id": {"type": "integer"}, "name": {"type": "string"} },
        "required": ["id", "name"]
    })).unwrap();

    for i in 0..5 {
        let mut tx = strunk.begin().await.unwrap();
        strunk.event(&mut tx, "item", &i.to_string())
            .state(serde_json::json!({"id": i, "name": format!("item-{}", i)}))
            .schema_version("1.0")
            .publish().await.unwrap();
        tx.commit().await.unwrap();
    }

    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_clone = seen.clone();

    let sub = strunk.subscriber("cursor-test", "item")
        .poll_interval(Duration::from_millis(50))
        .spawn(move |event| {
            let seen = seen_clone.clone();
            async move {
                seen.lock().await.push(event.entity_id.clone());
                Ok(())
            }
        });

    tokio::time::sleep(Duration::from_millis(500)).await;
    sub.abort();

    let first_batch = seen.lock().await.clone();
    assert_eq!(first_batch.len(), 5, "should have seen all 5 changes");

    for i in 5..8 {
        let mut tx = strunk.begin().await.unwrap();
        strunk.event(&mut tx, "item", &i.to_string())
            .state(serde_json::json!({"id": i, "name": format!("item-{}", i)}))
            .schema_version("1.0")
            .publish().await.unwrap();
        tx.commit().await.unwrap();
    }

    let seen2 = Arc::new(Mutex::new(Vec::new()));
    let seen2_clone = seen2.clone();

    let sub2 = strunk.subscriber("cursor-test", "item")
        .poll_interval(Duration::from_millis(50))
        .spawn(move |event| {
            let seen = seen2_clone.clone();
            async move {
                seen.lock().await.push(event.entity_id.clone());
                Ok(())
            }
        });

    tokio::time::sleep(Duration::from_millis(500)).await;
    sub2.abort();

    let second_batch = seen2.lock().await.clone();
    assert_eq!(second_batch.len(), 3, "should only see 3 new changes, not replay old ones");
}

#[tokio::test]
async fn event_feed_ordering_per_entity() {
    let Some(mut strunk) = setup().await else { return };

    strunk.register_schema("counter", "1.0", &serde_json::json!({
        "properties": { "value": {"type": "integer"} },
        "required": ["value"]
    })).unwrap();

    for i in 0..10 {
        let mut tx = strunk.begin().await.unwrap();
        strunk.event(&mut tx, "counter", "1")
            .state(serde_json::json!({"value": i}))
            .schema_version("1.0")
            .publish().await.unwrap();
        tx.commit().await.unwrap();
    }

    let values = Arc::new(Mutex::new(Vec::new()));
    let values_clone = values.clone();

    let sub = strunk.subscriber("order-test", "counter")
        .poll_interval(Duration::from_millis(50))
        .spawn(move |event| {
            let values = values_clone.clone();
            async move {
                values.lock().await.push(event.state["value"].as_i64().unwrap());
                Ok(())
            }
        });

    tokio::time::sleep(Duration::from_millis(500)).await;
    sub.abort();

    let values = values.lock().await;
    assert_eq!(values.len(), 10);
    for i in 0..10 {
        assert_eq!(values[i], i as i64, "changes should arrive in order");
    }
}

#[tokio::test]
async fn schema_type_mismatch_rejected() {
    let Some(mut strunk) = setup().await else { return };

    strunk.register_schema("typed", "1.0", &serde_json::json!({
        "properties": {
            "count": {"type": "integer"},
            "name": {"type": "string"}
        },
        "required": ["count", "name"]
    })).unwrap();

    let mut tx = strunk.begin().await.unwrap();
    let result = strunk.event(&mut tx, "typed", "1")
        .state(serde_json::json!({"count": "not_a_number", "name": "test"}))
        .schema_version("1.0")
        .publish().await;

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("count"));
}

#[tokio::test]
async fn schema_new_required_field_incompatible() {
    let Some(mut strunk) = setup().await else { return };

    strunk.register_schema("evolve", "1.0", &serde_json::json!({
        "properties": { "id": {"type": "integer"} },
        "required": ["id"]
    })).unwrap();

    let result = strunk.register_schema("evolve", "1.1", &serde_json::json!({
        "properties": {
            "id": {"type": "integer"},
            "mandatory_new": {"type": "string"}
        },
        "required": ["id", "mandatory_new"]
    }));

    assert!(result.is_err(), "adding a new required field should be incompatible");
}

#[tokio::test]
async fn reaper_does_not_touch_active_rows() {
    let Some(strunk) = setup().await else { return };

    let mut tx = strunk.begin().await.unwrap();
    for i in 0..5 {
        strunk.task(&mut tx, "reaper-safe-queue")
            .payload(serde_json::json!({"i": i}))
            .submit().await.unwrap();
    }
    tx.commit().await.unwrap();

    let reaper_handle = strunk.reaper()
        .retention_delivered(Duration::from_millis(1))
        .retention_dead(Duration::from_millis(1))
        .interval(Duration::from_millis(100))
        .spawn();

    tokio::time::sleep(Duration::from_millis(500)).await;
    reaper_handle.abort();

    let count = strunk.pending_count("reaper-safe-queue").await.unwrap();
    assert_eq!(count, 5, "reaper should not delete pending tasks");
}

#[tokio::test]
async fn snapshot_returns_latest_state() {
    let Some(mut strunk) = setup().await else { return };

    strunk.register_schema("doc", "1.0", &serde_json::json!({
        "properties": { "version": {"type": "integer"} },
        "required": ["version"]
    })).unwrap();

    for v in 1..=5 {
        let mut tx = strunk.begin().await.unwrap();
        strunk.event(&mut tx, "doc", "1")
            .state(serde_json::json!({"version": v}))
            .schema_version("1.0")
            .publish().await.unwrap();
        tx.commit().await.unwrap();
    }

    let state = strunk.snapshot("doc", "1").await.unwrap().unwrap();
    assert_eq!(state["version"], 5, "snapshot should reflect latest publish");
}

#[tokio::test]
async fn snapshot_missing_entity_returns_none() {
    let Some(strunk) = setup().await else { return };

    let state = strunk.snapshot("nonexistent", "999").await.unwrap();
    assert!(state.is_none());
}

#[tokio::test]
async fn batch_submit_empty() {
    let Some(strunk) = setup().await else { return };

    let mut tx = strunk.begin().await.unwrap();
    let ids = strunk.submit_batch(&mut tx, vec![]).await.unwrap();
    tx.commit().await.unwrap();

    assert!(ids.is_empty());
}

#[tokio::test]
async fn multiple_dead_letter_cycles() {
    let Some(strunk) = setup().await else { return };

    let mut tx = strunk.begin().await.unwrap();
    strunk.task(&mut tx, "cycle-queue")
        .payload(serde_json::json!({"stubborn": true}))
        .max_retries(1)
        .submit().await.unwrap();
    tx.commit().await.unwrap();

    for cycle in 0..3 {
        let task = strunk.claim("cycle-queue", Duration::from_secs(30)).await.unwrap()
            .unwrap_or_else(|| panic!("should be claimable on cycle {}", cycle));
        strunk.fail(task.id, 1, task.attempts).await.unwrap();

        let dead = strunk.dead_letters("cycle-queue", 10).await.unwrap();
        assert_eq!(dead.len(), 1);

        strunk.retry_dead(task.id).await.unwrap();
    }
}

#[tokio::test]
async fn fail_with_retries_remaining_reschedules() {
    let Some(strunk) = setup().await else { return };

    let mut tx = strunk.begin().await.unwrap();
    strunk.task(&mut tx, "retry-queue")
        .payload(serde_json::json!({"retryable": true}))
        .max_retries(5)
        .submit().await.unwrap();
    tx.commit().await.unwrap();

    let task = strunk.claim("retry-queue", Duration::from_secs(30)).await.unwrap().unwrap();
    strunk.fail(task.id, 5, task.attempts).await.unwrap();

    let dead = strunk.dead_letters("retry-queue", 10).await.unwrap();
    assert!(dead.is_empty(), "should not be dead-lettered with retries remaining");
}
