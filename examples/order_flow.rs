use std::time::Duration;

use strunk::{Strunk, StrunkConfig};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let database_url =
        std::env::var("DATABASE_URL").unwrap_or("postgres://localhost/strunk_example".to_string());

    let config = StrunkConfig {
        database_url,
        ..Default::default()
    };

    let mut strunk = Strunk::new(config).await?;
    strunk.migrate().await?;

    strunk.register_schema(
        "order",
        "1.0",
        &serde_json::json!({
            "properties": {
                "id": { "type": "integer" },
                "status": { "type": "string" },
                "total": { "type": "number" },
                "customer": { "type": "string" }
            },
            "required": ["id", "status", "total"]
        }),
    )?;

    let mut tx = strunk.begin().await?;

    let task_id = strunk
        .task(&mut tx, "email-notifications")
        .payload(serde_json::json!({
            "to": "customer@example.com",
            "template": "order_confirmed",
            "order_id": 42
        }))
        .priority(5)
        .max_retries(3)
        .submit()
        .await?;

    println!("submitted task {}", task_id);

    let event_id = strunk
        .event(&mut tx, "order", "42")
        .state(serde_json::json!({
            "id": 42,
            "status": "confirmed",
            "total": 59.99,
            "customer": "jane@example.com"
        }))
        .diff(serde_json::json!({ "changed": ["status"] }))
        .schema_version("1.0")
        .publish()
        .await?;

    println!("published event {}", event_id);

    tx.commit().await?;
    println!("transaction committed: task and event are now visible");

    let worker_handles = strunk
        .worker("email-notifications")
        .concurrency(2)
        .visibility_timeout(Duration::from_secs(30))
        .spawn(|task| async move {
            println!(
                "processing task {} from queue '{}': {}",
                task.id, task.queue, task.payload
            );
            Ok(())
        });

    let _subscriber = strunk
        .subscriber("search-indexer", "order")
        .spawn(|event| async move {
            println!(
                "order {} is now: {}",
                event.entity_id, event.state
            );
            Ok(())
        });

    tokio::time::sleep(Duration::from_secs(1)).await;

    let state = strunk.snapshot("order", "42").await?;
    println!("current snapshot for order 42: {:?}", state);

    let pending = strunk.pending_count("email-notifications").await?;
    println!("pending tasks in email-notifications: {}", pending);

    for handle in worker_handles {
        handle.abort();
    }

    println!("done");
    Ok(())
}
