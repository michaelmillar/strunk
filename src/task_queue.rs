use std::future::Future;
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;
use sqlx::{PgPool, Postgres, Transaction};
use sqlx::postgres::PgListener;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

use crate::error::Result;
use crate::relay;
use crate::types::{OutboxRow, Task, TypedTask};

pub struct TaskSubmit<'a> {
    tx: &'a mut Transaction<'static, Postgres>,
    queue: String,
    payload: serde_json::Value,
    metadata: serde_json::Value,
    priority: i32,
    max_retries: i32,
    delay: Option<Duration>,
    dedup_key: Option<String>,
}

impl<'a> TaskSubmit<'a> {
    pub fn new(tx: &'a mut Transaction<'static, Postgres>, queue: impl Into<String>) -> Self {
        Self {
            tx,
            queue: queue.into(),
            payload: serde_json::Value::Null,
            metadata: serde_json::json!({}),
            priority: 0,
            max_retries: 3,
            delay: None,
            dedup_key: None,
        }
    }

    pub fn payload(mut self, payload: serde_json::Value) -> Self {
        self.payload = payload;
        self
    }

    pub fn metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    pub fn max_retries(mut self, max_retries: i32) -> Self {
        self.max_retries = max_retries;
        self
    }

    pub fn delay(mut self, delay: Duration) -> Self {
        self.delay = Some(delay);
        self
    }

    pub fn dedup_key(mut self, key: impl Into<String>) -> Self {
        self.dedup_key = Some(key.into());
        self
    }

    pub fn typed<T: Serialize>(mut self, data: &T) -> Self {
        self.payload = serde_json::to_value(data).expect("payload serialisation failed");
        self
    }

    pub async fn submit(self) -> Result<i64> {
        let delay_secs = self.delay.map(|d| d.as_secs_f64()).unwrap_or(0.0);

        let row = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO strunk_outbox (kind, key, dedup_key, payload, metadata, priority, max_retries, visible_at)
            VALUES ('task', $1, $2, $3, $4, $5, $6, now() + make_interval(secs => $7::double precision))
            RETURNING id
            "#,
        )
        .bind(&self.queue)
        .bind(&self.dedup_key)
        .bind(&self.payload)
        .bind(&self.metadata)
        .bind(self.priority)
        .bind(self.max_retries)
        .bind(delay_secs)
        .fetch_one(&mut **self.tx)
        .await?;

        Ok(row)
    }
}

pub struct BatchItem {
    pub queue: String,
    pub payload: serde_json::Value,
    pub metadata: serde_json::Value,
    pub priority: i32,
    pub max_retries: i32,
    pub delay_secs: f64,
}

impl BatchItem {
    pub fn new(queue: impl Into<String>, payload: serde_json::Value) -> Self {
        Self {
            queue: queue.into(),
            payload,
            metadata: serde_json::json!({}),
            priority: 0,
            max_retries: 3,
            delay_secs: 0.0,
        }
    }

    pub fn priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    pub fn max_retries(mut self, max_retries: i32) -> Self {
        self.max_retries = max_retries;
        self
    }

    pub fn delay(mut self, delay: Duration) -> Self {
        self.delay_secs = delay.as_secs_f64();
        self
    }
}

pub async fn submit_batch(
    tx: &mut Transaction<'static, Postgres>,
    items: Vec<BatchItem>,
) -> Result<Vec<i64>> {
    if items.is_empty() {
        return Ok(vec![]);
    }

    let mut keys = Vec::with_capacity(items.len());
    let mut payloads = Vec::with_capacity(items.len());
    let mut metadatas = Vec::with_capacity(items.len());
    let mut priorities = Vec::with_capacity(items.len());
    let mut retries = Vec::with_capacity(items.len());
    let mut delays = Vec::with_capacity(items.len());

    for item in &items {
        keys.push(item.queue.as_str());
        payloads.push(&item.payload);
        metadatas.push(&item.metadata);
        priorities.push(item.priority);
        retries.push(item.max_retries);
        delays.push(item.delay_secs);
    }

    let ids = sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO strunk_outbox (kind, key, payload, metadata, priority, max_retries, visible_at)
        SELECT 'task', key, payload, metadata, priority, max_retries,
               now() + make_interval(secs => delay::double precision)
        FROM unnest($1::text[], $2::jsonb[], $3::jsonb[], $4::int[], $5::int[], $6::float8[])
            AS t(key, payload, metadata, priority, max_retries, delay)
        RETURNING id
        "#,
    )
    .bind(&keys)
    .bind(&payloads)
    .bind(&metadatas)
    .bind(&priorities)
    .bind(&retries)
    .bind(&delays)
    .fetch_all(&mut **tx)
    .await?;

    Ok(ids)
}

pub trait Middleware: Send + Sync + 'static {
    fn before(&self, task: &Task) {
        let _ = task;
    }

    fn after_success(&self, task_id: i64, duration: Duration) {
        let _ = (task_id, duration);
    }

    fn after_failure(&self, task_id: i64, duration: Duration, error: &str) {
        let _ = (task_id, duration, error);
    }
}

pub struct LoggingMiddleware;

impl Middleware for LoggingMiddleware {
    fn before(&self, task: &Task) {
        tracing::info!(task_id = task.id, queue = %task.queue, "starting task");
    }

    fn after_success(&self, task_id: i64, duration: Duration) {
        tracing::info!(task_id, duration_ms = duration.as_millis() as u64, "task completed");
    }

    fn after_failure(&self, task_id: i64, duration: Duration, error: &str) {
        tracing::warn!(task_id, duration_ms = duration.as_millis() as u64, error, "task failed");
    }
}

pub async fn claim(pool: &PgPool, queue: &str, visibility_timeout: Duration) -> Result<Option<Task>> {
    let timeout_secs = visibility_timeout.as_secs_f64();

    let row = sqlx::query_as::<_, OutboxRow>(
        r#"
        UPDATE strunk_outbox
        SET status = 'claimed',
            visible_at = now() + make_interval(secs => $2::double precision),
            attempts = attempts + 1
        WHERE id = (
            SELECT id FROM strunk_outbox
            WHERE kind = 'task'
            AND key = $1
            AND status IN ('pending', 'claimed')
            AND visible_at <= now()
            ORDER BY priority DESC, id
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        RETURNING *
        "#,
    )
    .bind(queue)
    .bind(timeout_secs)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(Task::from))
}

pub async fn complete(pool: &PgPool, task_id: i64) -> Result<()> {
    relay::mark_delivered(pool, task_id).await
}

pub async fn complete_with_result(
    pool: &PgPool,
    task_id: i64,
    queue: &str,
    result: serde_json::Value,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO strunk_results (task_id, queue, result)
        VALUES ($1, $2, $3)
        ON CONFLICT (task_id) DO UPDATE SET result = $3, completed_at = now()
        "#,
    )
    .bind(task_id)
    .bind(queue)
    .bind(&result)
    .execute(pool)
    .await?;

    relay::mark_delivered(pool, task_id).await
}

pub async fn get_result(pool: &PgPool, task_id: i64) -> Result<Option<crate::types::TaskResult>> {
    let row = sqlx::query_as::<_, (i64, String, serde_json::Value, chrono::DateTime<chrono::Utc>)>(
        "SELECT task_id, queue, result, completed_at FROM strunk_results WHERE task_id = $1",
    )
    .bind(task_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|(task_id, queue, result, completed_at)| crate::types::TaskResult {
        task_id,
        queue,
        result,
        completed_at,
    }))
}

pub async fn fail(pool: &PgPool, task_id: i64, max_retries: i32, attempts: i32) -> Result<()> {
    relay::mark_failed(pool, task_id, max_retries, attempts).await
}

pub async fn heartbeat(pool: &PgPool, task_id: i64, extend_by: Duration) -> Result<()> {
    sqlx::query(
        "UPDATE strunk_outbox SET visible_at = now() + make_interval(secs => $2::double precision) WHERE id = $1 AND status = 'claimed'",
    )
    .bind(task_id)
    .bind(extend_by.as_secs_f64())
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn set_progress(pool: &PgPool, task_id: i64, progress: i16) -> Result<()> {
    sqlx::query(
        "UPDATE strunk_outbox SET progress = $2 WHERE id = $1 AND status = 'claimed'",
    )
    .bind(task_id)
    .bind(progress.clamp(0, 100))
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn get_progress(pool: &PgPool, task_id: i64) -> Result<Option<i16>> {
    let row = sqlx::query_scalar::<_, i16>(
        "SELECT progress FROM strunk_outbox WHERE id = $1",
    )
    .bind(task_id)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

pub async fn inbox_contains(pool: &PgPool, consumer_id: &str, message_id: i64) -> Result<bool> {
    let count = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM strunk_inbox WHERE consumer_id = $1 AND message_id = $2",
    )
    .bind(consumer_id)
    .bind(message_id)
    .fetch_one(pool)
    .await?;
    Ok(count > 0)
}

pub async fn inbox_mark(pool: &PgPool, consumer_id: &str, message_id: i64) -> Result<()> {
    sqlx::query(
        "INSERT INTO strunk_inbox (consumer_id, message_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
    )
    .bind(consumer_id)
    .bind(message_id)
    .execute(pool)
    .await?;
    Ok(())
}

pub struct Worker {
    pool: PgPool,
    queue: String,
    database_url: Option<String>,
    concurrency: usize,
    visibility_timeout: Duration,
    poll_interval: Duration,
    token: CancellationToken,
    middleware: Option<std::sync::Arc<dyn Middleware>>,
    inbox_id: Option<String>,
}

impl Worker {
    pub fn new(pool: PgPool, queue: impl Into<String>) -> Self {
        Self {
            pool,
            queue: queue.into(),
            database_url: None,
            concurrency: 1,
            visibility_timeout: Duration::from_secs(30),
            poll_interval: Duration::from_millis(100),
            token: CancellationToken::new(),
            middleware: None,
            inbox_id: None,
        }
    }

    pub fn concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    pub fn visibility_timeout(mut self, timeout: Duration) -> Self {
        self.visibility_timeout = timeout;
        self
    }

    pub fn poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    pub fn cancellation_token(mut self, token: CancellationToken) -> Self {
        self.token = token;
        self
    }

    pub fn middleware(mut self, mw: impl Middleware) -> Self {
        self.middleware = Some(std::sync::Arc::new(mw));
        self
    }

    pub fn database_url(mut self, url: impl Into<String>) -> Self {
        self.database_url = Some(url.into());
        self
    }

    pub fn inbox(mut self, consumer_id: impl Into<String>) -> Self {
        self.inbox_id = Some(consumer_id.into());
        self
    }

    pub fn spawn_typed<T, F, Fut>(self, handler: F) -> Vec<JoinHandle<()>>
    where
        T: DeserializeOwned + Send + 'static,
        F: Fn(TypedTask<T>) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send,
    {
        self.spawn(move |task| {
            let handler = handler.clone();
            async move {
                let typed = TypedTask::try_from_task(task)
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                handler(typed).await
            }
        })
    }

    pub fn spawn<F, Fut>(self, handler: F) -> Vec<JoinHandle<()>>
    where
        F: Fn(Task) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send,
    {
        let mut handles = Vec::with_capacity(self.concurrency);

        for worker_id in 0..self.concurrency {
            let pool = self.pool.clone();
            let queue = self.queue.clone();
            let database_url = self.database_url.clone();
            let handler = handler.clone();
            let visibility_timeout = self.visibility_timeout;
            let poll_interval = self.poll_interval;
            let token = self.token.clone();
            let middleware = self.middleware.clone();
            let inbox_id = self.inbox_id.clone();

            let handle = tokio::spawn(async move {
                let expected_prefix = format!("task:{}", queue);
                let mut listener = match &database_url {
                    Some(url) => match PgListener::connect(url).await {
                        Ok(mut l) => {
                            if l.listen("strunk").await.is_ok() {
                                Some(l)
                            } else {
                                None
                            }
                        }
                        Err(_) => None,
                    },
                    None => None,
                };

                loop {
                    if token.is_cancelled() {
                        debug!(worker_id, queue = %queue, "worker shutting down");
                        return;
                    }

                    match claim(&pool, &queue, visibility_timeout).await {
                        Ok(Some(task)) => {
                            let task_id = task.id;
                            let max_retries = task.max_retries;
                            let attempts = task.attempts;

                            if let Some(ref cid) = inbox_id {
                                if inbox_contains(&pool, cid, task_id).await.unwrap_or(false) {
                                    debug!(task_id, "skipping already-processed task (inbox)");
                                    if let Err(e) = complete(&pool, task_id).await {
                                        error!(task_id, error = %e, "failed to mark inbox-skipped task complete");
                                    }
                                    continue;
                                }
                            }

                            if let Some(ref mw) = middleware {
                                mw.before(&task);
                            }

                            let start = tokio::time::Instant::now();

                            match handler(task).await {
                                Ok(()) => {
                                    if let Some(ref cid) = inbox_id {
                                        if let Err(e) = inbox_mark(&pool, cid, task_id).await {
                                            error!(task_id, error = %e, "failed to mark inbox");
                                        }
                                    }
                                    if let Some(ref mw) = middleware {
                                        mw.after_success(task_id, start.elapsed());
                                    }
                                    if let Err(e) = complete(&pool, task_id).await {
                                        error!(task_id, error = %e, "failed to mark task complete");
                                    }
                                }
                                Err(e) => {
                                    if let Some(ref mw) = middleware {
                                        mw.after_failure(task_id, start.elapsed(), &e.to_string());
                                    }
                                    warn!(task_id, error = %e, "task handler failed");
                                    if let Err(e) = fail(&pool, task_id, max_retries, attempts).await {
                                        error!(task_id, error = %e, "failed to mark task failed");
                                    }
                                }
                            }
                        }
                        Ok(None) => {
                            if let Some(ref mut l) = listener {
                                tokio::select! {
                                    notification = l.recv() => {
                                        if let Ok(n) = notification {
                                            if !n.payload().starts_with(&expected_prefix) {
                                                tokio::time::sleep(Duration::from_millis(1)).await;
                                            }
                                        }
                                    }
                                    _ = tokio::time::sleep(poll_interval) => {}
                                    _ = token.cancelled() => {
                                        debug!(worker_id, queue = %queue, "worker shutting down");
                                        return;
                                    }
                                }
                            } else {
                                tokio::select! {
                                    _ = tokio::time::sleep(poll_interval) => {}
                                    _ = token.cancelled() => {
                                        debug!(worker_id, queue = %queue, "worker shutting down");
                                        return;
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            error!(worker_id, error = %e, "claim failed");
                            tokio::select! {
                                _ = tokio::time::sleep(poll_interval) => {}
                                _ = token.cancelled() => return,
                            }
                        }
                    }
                }
            });

            handles.push(handle);
        }

        handles
    }
}
