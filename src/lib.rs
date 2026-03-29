pub mod events;
pub mod config;
pub mod error;
pub mod health;
pub mod migrate;
pub mod reaper;
pub mod relay;
pub mod scheduler;
pub mod schema;
pub mod stats;
pub mod task_queue;
pub mod types;

pub use config::StrunkConfig;
pub use error::{Result, StrunkError};
pub use health::HealthReport;
pub use scheduler::{Schedule, ScheduleBuilder};
pub use stats::{OverallStats, QueueStats, SubscriberStats};
pub use task_queue::{BatchItem, LoggingMiddleware, Middleware};
pub use types::{MessageKind, MessageStatus, OutboxRow, StateEvent, Task, TaskResult, TypedStateEvent, TypedTask};

use std::time::Duration;

use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Postgres, Transaction};
use tokio_util::sync::CancellationToken;

use crate::events::{EventPublish, Subscriber};
use crate::reaper::Reaper;
use crate::schema::SchemaRegistry;
use crate::task_queue::{TaskSubmit, Worker};

pub struct Strunk {
    pool: PgPool,
    config: StrunkConfig,
    registry: SchemaRegistry,
    token: CancellationToken,
}

impl Strunk {
    pub async fn new(config: StrunkConfig) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(20)
            .connect(&config.database_url)
            .await?;

        Ok(Self {
            pool,
            config,
            registry: SchemaRegistry::new(),
            token: CancellationToken::new(),
        })
    }

    pub fn from_pool(pool: PgPool, config: StrunkConfig) -> Self {
        Self {
            pool,
            config,
            registry: SchemaRegistry::new(),
            token: CancellationToken::new(),
        }
    }

    pub async fn migrate(&self) -> Result<()> {
        migrate::migrate(&self.pool).await
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.token.clone()
    }

    pub fn shutdown(&self) {
        self.token.cancel();
    }

    pub fn register_schema(
        &mut self,
        entity_type: &str,
        version: &str,
        schema: &serde_json::Value,
    ) -> Result<()> {
        self.registry.register(entity_type, version, schema)
    }

    pub fn task<'a>(
        &self,
        tx: &'a mut Transaction<'static, Postgres>,
        queue: impl Into<String>,
    ) -> TaskSubmit<'a> {
        TaskSubmit::new(tx, queue)
    }

    pub async fn submit_batch(
        &self,
        tx: &mut Transaction<'static, Postgres>,
        items: Vec<BatchItem>,
    ) -> Result<Vec<i64>> {
        task_queue::submit_batch(tx, items).await
    }

    pub fn event<'a>(
        &'a self,
        tx: &'a mut Transaction<'static, Postgres>,
        entity_type: impl Into<String>,
        entity_id: impl Into<String>,
    ) -> EventPublish<'a> {
        EventPublish::new(tx, entity_type, entity_id).with_registry(&self.registry)
    }

    pub async fn snapshot(
        &self,
        entity_type: &str,
        entity_id: &str,
    ) -> Result<Option<serde_json::Value>> {
        events::snapshot(&self.pool, entity_type, entity_id).await
    }

    pub fn subscriber(
        &self,
        id: impl Into<String>,
        entity_type: impl Into<String>,
    ) -> Subscriber {
        Subscriber::new(self.pool.clone(), id, entity_type)
            .database_url(self.config.database_url.clone())
            .cancellation_token(self.token.clone())
    }

    pub async fn claim(
        &self,
        queue: &str,
        visibility_timeout: Duration,
    ) -> Result<Option<types::Task>> {
        task_queue::claim(&self.pool, queue, visibility_timeout).await
    }

    pub async fn complete(&self, task_id: i64) -> Result<()> {
        task_queue::complete(&self.pool, task_id).await
    }

    pub async fn complete_with_result(
        &self,
        task_id: i64,
        queue: &str,
        result: serde_json::Value,
    ) -> Result<()> {
        task_queue::complete_with_result(&self.pool, task_id, queue, result).await
    }

    pub async fn get_result(&self, task_id: i64) -> Result<Option<TaskResult>> {
        task_queue::get_result(&self.pool, task_id).await
    }

    pub async fn fail(&self, task_id: i64, max_retries: i32, attempts: i32) -> Result<()> {
        task_queue::fail(&self.pool, task_id, max_retries, attempts).await
    }

    pub async fn heartbeat(&self, task_id: i64, extend_by: Duration) -> Result<()> {
        task_queue::heartbeat(&self.pool, task_id, extend_by).await
    }

    pub async fn set_progress(&self, task_id: i64, progress: i16) -> Result<()> {
        task_queue::set_progress(&self.pool, task_id, progress).await
    }

    pub async fn get_progress(&self, task_id: i64) -> Result<Option<i16>> {
        task_queue::get_progress(&self.pool, task_id).await
    }

    pub fn worker(&self, queue: impl Into<String>) -> Worker {
        Worker::new(self.pool.clone(), queue)
            .database_url(self.config.database_url.clone())
            .cancellation_token(self.token.clone())
    }

    pub fn reaper(&self) -> Reaper {
        Reaper::new(self.pool.clone())
            .retention_delivered(self.config.reaper_retention_delivered)
            .retention_dead(self.config.reaper_retention_dead)
            .batch_size(self.config.reaper_batch_size)
            .interval(self.config.reaper_interval)
            .cancellation_token(self.token.clone())
    }

    pub fn relay(&self) -> relay::Relay {
        relay::Relay::new(
            self.pool.clone(),
            self.config.poll_interval,
            self.config.relay_batch_size,
        )
        .cancellation_token(self.token.clone())
    }

    pub fn schedule(
        &self,
        id: impl Into<String>,
        queue: impl Into<String>,
        cron: impl Into<String>,
    ) -> ScheduleBuilder {
        ScheduleBuilder::new(self.pool.clone(), id, queue, cron)
    }

    pub fn scheduler(&self) -> scheduler::Scheduler {
        scheduler::Scheduler::new(self.pool.clone())
            .cancellation_token(self.token.clone())
    }

    pub async fn list_schedules(&self) -> Result<Vec<Schedule>> {
        scheduler::list(&self.pool).await
    }

    pub async fn disable_schedule(&self, id: &str) -> Result<()> {
        scheduler::disable(&self.pool, id).await
    }

    pub async fn enable_schedule(&self, id: &str) -> Result<()> {
        scheduler::enable(&self.pool, id).await
    }

    pub async fn remove_schedule(&self, id: &str) -> Result<()> {
        scheduler::remove(&self.pool, id).await
    }

    pub async fn begin(&self) -> Result<Transaction<'static, Postgres>> {
        Ok(self.pool.begin().await?)
    }

    pub async fn dead_letters(&self, queue: &str, limit: i64) -> Result<Vec<types::OutboxRow>> {
        let rows = sqlx::query_as::<_, types::OutboxRow>(
            r#"
            SELECT * FROM strunk_outbox
            WHERE status = 'dead' AND key = $1
            ORDER BY created_at DESC
            LIMIT $2
            "#,
        )
        .bind(queue)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    pub async fn retry_dead(&self, task_id: i64) -> Result<()> {
        sqlx::query(
            "UPDATE strunk_outbox SET status = 'pending', attempts = 0, visible_at = now() WHERE id = $1 AND status = 'dead'",
        )
        .bind(task_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn pending_count(&self, queue: &str) -> Result<i64> {
        let count = sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM strunk_outbox WHERE key = $1 AND status IN ('pending', 'claimed')",
        )
        .bind(queue)
        .fetch_one(&self.pool)
        .await?;

        Ok(count)
    }

    pub async fn queue_stats(&self, queue: &str) -> Result<stats::QueueStats> {
        stats::queue_stats(&self.pool, queue).await
    }

    pub async fn subscriber_stats(&self, subscriber_id: &str) -> Result<Option<stats::SubscriberStats>> {
        stats::subscriber_stats(&self.pool, subscriber_id).await
    }

    pub async fn overall_stats(&self) -> Result<stats::OverallStats> {
        stats::overall_stats(&self.pool).await
    }

    pub async fn health(&self, max_pending_age: Duration) -> Result<HealthReport> {
        health::check(&self.pool, max_pending_age).await
    }

    pub async fn replay_subscriber(&self, subscriber_id: &str, from_id: i64) -> Result<()> {
        sqlx::query("UPDATE strunk_subscribers SET last_seen_id = $2 WHERE id = $1")
            .bind(subscriber_id)
            .bind(from_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn reset_subscriber(&self, subscriber_id: &str) -> Result<()> {
        self.replay_subscriber(subscriber_id, 0).await
    }

    pub async fn inbox_contains(&self, consumer_id: &str, message_id: i64) -> Result<bool> {
        task_queue::inbox_contains(&self.pool, consumer_id, message_id).await
    }

    pub async fn inbox_mark(&self, consumer_id: &str, message_id: i64) -> Result<()> {
        task_queue::inbox_mark(&self.pool, consumer_id, message_id).await
    }
}
