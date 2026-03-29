<p align="center">
  <img src="assets/icon.svg" alt="strunk icon" width="120"/>
  <br/><br/>
  <img src="assets/logo.svg" alt="strunk" width="500"/>
</p>

<p align="center">
  Durable background jobs and state events for PostgreSQL-backed Rust services.
  <br/>
  Enqueue work and publish domain state in the same transaction as your business data.
</p>

---

https://github.com/user-attachments/assets/508b0ac3-ff11-4e2f-ac78-feff9b0e408e

## What it does

Strunk is a Rust library that gives your service durable task queues and entity state events using your existing PostgreSQL database. Both primitives write to an outbox table inside your normal database transactions. If the transaction rolls back, the work never existed. If it commits, delivery follows.

This eliminates the dual-write problem without introducing a message broker, and keeps your operational surface to one system you already run.

```rust
let mut tx = strunk.begin().await?;

sqlx::query("UPDATE orders SET status = 'shipped' WHERE id = $1")
    .bind(order_id)
    .execute(&mut *tx)
    .await?;

strunk.task(&mut tx, "notifications")
    .payload(json!({"order_id": order_id, "type": "shipped"}))
    .submit()
    .await?;

strunk.event(&mut tx, "order", &order_id.to_string())
    .state(json!({"id": order_id, "status": "shipped", "total": 59.99}))
    .publish()
    .await?;

tx.commit().await?;
```

All three writes happen atomically. Or none of them do.

## Delivery guarantees

Tasks are delivered **at least once**. A worker claims a task, processes it, and marks it complete. If the worker crashes, the visibility timeout expires and another worker reclaims it. This means your handlers must be idempotent for external side effects (HTTP calls, emails, webhooks). Use `dedup_key` to prevent duplicate submissions at enqueue time.

State events are delivered **at least once, in order per entity**. Subscribers track their own cursor. If a subscriber crashes, it resumes from its last acknowledged position. No events are skipped.

Neither primitive provides exactly-once delivery for effects outside the database. If your entire side effect is a database write in the same Postgres instance, you can achieve effectively-once by running it in a transaction. For anything external, design for at-least-once.

## What you are operating

Strunk adds several background loops to your process. These are not a broker cluster, but they are moving parts you should understand.

**Relay** polls for pending event rows and marks them delivered. If it stops, event delivery stalls but no data is lost.

**Reaper** deletes delivered and dead-lettered rows past their retention window. If it stops, the outbox table grows. Monitor `table_size` in stats.

**Scheduler** fires recurring tasks by inserting rows when schedules come due. Uses deduplication to prevent double-firing across multiple instances.

**Workers** claim and process tasks. If all workers stop, tasks accumulate as pending rows. Visibility timeouts ensure claimed-but-abandoned tasks resurface.

Workers and subscribers use PostgreSQL `LISTEN/NOTIFY` for instant wakeup when new work arrives. The poll interval acts as a fallback, not the primary delivery mechanism. This means near-zero latency in the common case without hammering the database with empty polls.

Your PostgreSQL instance bears the load of row locking, notification dispatch, and index maintenance. This is fine for moderate workloads (thousands of tasks per second on typical hardware). If your database is already your bottleneck, or you need sustained high-throughput fan-out, Strunk is not the right tool.

## Task queue

Submit, claim, complete, fail. At-least-once delivery, priority ordering, visibility timeouts, exponential backoff with jitter, dead-letter inspection.

```rust
strunk.task(&mut tx, "email-queue")
    .payload(json!({"to": "user@example.com"}))
    .priority(5)
    .max_retries(3)
    .dedup_key("welcome-user-42")
    .submit()
    .await?;

strunk.worker("email-queue")
    .concurrency(4)
    .spawn(|task| async move {
        send_email(&task.payload).await?;
        Ok(())
    });
```

### Typed handlers

Define your payload as a Rust struct. Serialisation at enqueue, deserialisation at claim, both checked at compile time.

```rust
#[derive(Serialize, Deserialize)]
struct SendEmail {
    to: String,
    template: String,
}

strunk.task(&mut tx, "emails")
    .typed(&SendEmail { to: "user@example.com".into(), template: "welcome".into() })
    .submit()
    .await?;

strunk.worker("emails")
    .spawn_typed(|task: TypedTask<SendEmail>| async move {
        send_email(&task.data.to, &task.data.template).await?;
        Ok(())
    });
```

If the payload cannot be deserialised into the expected type, the task fails immediately (poison message handling).

### Consumer inbox

Producer-side deduplication prevents duplicate submissions via `dedup_key`. The consumer inbox prevents duplicate processing. If a worker crashes after processing but before marking complete, the task will be reclaimed. With an inbox, the worker checks whether it already handled that task and skips the duplicate.

```rust
strunk.worker("payments")
    .inbox("payment-processor")
    .concurrency(4)
    .spawn(|task| async move {
        charge_card(&task.payload).await?;
        Ok(())
    });
```

The inbox is cleaned up automatically by the reaper alongside delivered rows.

## State events

Publish the current state of a domain entity inside your transaction. Subscribers track their own cursor and resume from where they left off. Snapshots give you the latest state without subscribing.

This is not CDC. Strunk does not stream database mutations. You explicitly publish state when your application decides something meaningful happened.

```rust
strunk.event(&mut tx, "order", "42")
    .state(json!({"id": 42, "status": "confirmed", "total": 59.99}))
    .schema_version("1.0")
    .publish()
    .await?;

strunk.subscriber("search-indexer", "order")
    .spawn(|event| async move {
        update_index(event.entity_id, &event.state).await?;
        Ok(())
    });

let state = strunk.snapshot("order", "42").await?;
```

Typed subscribers work the same way as typed workers:

```rust
strunk.subscriber("indexer", "order")
    .spawn_typed(|event: TypedStateEvent<Order>| async move {
        update_index(&event.entity_id, &event.data).await?;
        Ok(())
    });
```

### Replay

Subscribers can be rewound to reprocess events from any point. Useful when a subscriber had a bug and you need to reprocess after deploying the fix.

```rust
strunk.replay_subscriber("search-indexer", from_id).await?;
strunk.reset_subscriber("search-indexer").await?;
```

Or from the CLI:

```bash
strunk replay search-indexer --from 0
strunk reset-subscriber search-indexer
```

## Schema registry

Versioned contracts validated at publish time. Backward compatibility enforced automatically. Adding optional fields is fine. Removing required fields or changing types fails at registration.

```rust
strunk.register_schema("order", "1.0", &json!({
    "properties": {
        "id": {"type": "integer"},
        "status": {"type": "string"},
        "total": {"type": "number"}
    },
    "required": ["id", "status", "total"]
}))?;

strunk.register_schema("order", "1.1", &json!({
    "properties": {
        "id": {"type": "integer"},
        "status": {"type": "string"},
        "total": {"type": "number"},
        "notes": {"type": "string"}
    },
    "required": ["id", "status", "total"]
}))?;
```

## Observability

Everything is a SQL query.

```rust
let stats = strunk.queue_stats("email-queue").await?;
let sub = strunk.subscriber_stats("search-indexer").await?;
let overall = strunk.overall_stats().await?;
let report = strunk.health(Duration::from_secs(300)).await?;
```

Dead letters are rows, not a separate topic:

```sql
SELECT * FROM strunk_outbox WHERE status = 'dead' AND key = 'email-queue';
UPDATE strunk_outbox SET status = 'pending', attempts = 0 WHERE id = 12345;
```

## Batch submit

```rust
let items = orders.iter().map(|o| {
    BatchItem::new("fulfilment", json!({"order_id": o.id}))
        .priority(o.priority)
}).collect();

let mut tx = strunk.begin().await?;
let ids = strunk.submit_batch(&mut tx, items).await?;
tx.commit().await?;
```

## Recurring schedules

```rust
strunk.schedule("daily-report", "reports", "every 1d")
    .payload(json!({"type": "daily"}))
    .priority(3)
    .register()
    .await?;

strunk.scheduler().spawn();
```

Supports `every 30s`, `every 5m`, `every 1h`, `every 1d`, `@hourly`, `@daily`, `@weekly`.

## Worker middleware

```rust
strunk.worker("email-queue")
    .middleware(LoggingMiddleware)
    .concurrency(4)
    .spawn(|task| async move {
        send_email(&task.payload).await?;
        Ok(())
    });
```

## Graceful shutdown

All background loops share a cancellation token.

```rust
let handles = strunk.worker("queue").spawn(handler);
let _sub = strunk.subscriber("indexer", "order").spawn(on_event);
let _reaper = strunk.reaper().spawn();

strunk.shutdown();
for h in handles { h.await.ok(); }
```

## CLI

```bash
cargo install strunk --features cli
```

```bash
strunk --database-url postgres://localhost/mydb migrate
strunk --database-url postgres://localhost/mydb stats
strunk --database-url postgres://localhost/mydb stats --queue email-queue
strunk --database-url postgres://localhost/mydb dead-letters email-queue
strunk --database-url postgres://localhost/mydb retry 12345
strunk --database-url postgres://localhost/mydb retry-all email-queue
strunk --database-url postgres://localhost/mydb subscribers
strunk --database-url postgres://localhost/mydb lag search-indexer
strunk --database-url postgres://localhost/mydb health
strunk --database-url postgres://localhost/mydb replay search-indexer --from 0
strunk --database-url postgres://localhost/mydb reset-subscriber search-indexer
```

Or set `DATABASE_URL` in your environment and omit the flag.

## When to use something else

Strunk is not a fit if you need:

- **High-throughput stream processing.** Windowed aggregations, stream joins, sustained millions of messages per second. Use Kafka or Redpanda.
- **Database mutation streaming (CDC).** Capturing every row change via logical decoding. Use Debezium or Sequin.
- **Durable workflow orchestration.** Long-running sagas with compensation, timers, and human-in-the-loop steps. Use Temporal.
- **Global total ordering.** Strunk orders per entity in the event stream and by priority in task queues. It does not provide a single global order across all messages.
- **Event sourcing.** Strunk publishes current state, not a sequence of domain events that reconstruct state. If you need event replay to rebuild aggregates, this is the wrong model.

The best use of Strunk is a Rust service (or small set of services) on PostgreSQL that needs reliable background work, domain state propagation, and transactional safety, without adopting a separate broker or workflow platform.

## Running the example

```bash
export DATABASE_URL="postgres://localhost/strunk_example"
createdb strunk_example
cargo run --example order_flow
```

## Running tests

```bash
export DATABASE_URL="postgres://localhost/strunk_test"
createdb strunk_test
cargo test
```

## Licence

MIT
