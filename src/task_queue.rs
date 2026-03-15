use std::future::Future;
use std::time::Duration;

use sqlx::{PgPool, Postgres, Transaction};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

use crate::error::Result;
use crate::relay;
use crate::types::{OutboxRow, Task};

pub struct TaskSubmit<'a> {
    tx: &'a mut Transaction<'static, Postgres>,
    queue: String,
    payload: serde_json::Value,
    metadata: serde_json::Value,
    priority: i32,
    max_retries: i32,
    delay: Option<Duration>,
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

    pub async fn submit(self) -> Result<i64> {
        let delay_secs = self.delay.map(|d| d.as_secs_f64()).unwrap_or(0.0);

        let row = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO strunk_outbox (kind, key, payload, metadata, priority, max_retries, visible_at)
            VALUES ('task', $1, $2, $3, $4, $5, now() + make_interval(secs => $6::double precision))
            RETURNING id
            "#,
        )
        .bind(&self.queue)
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

pub async fn fail(pool: &PgPool, task_id: i64, max_retries: i32, attempts: i32) -> Result<()> {
    relay::mark_failed(pool, task_id, max_retries, attempts).await
}

pub struct Worker {
    pool: PgPool,
    queue: String,
    concurrency: usize,
    visibility_timeout: Duration,
    poll_interval: Duration,
    token: CancellationToken,
}

impl Worker {
    pub fn new(pool: PgPool, queue: impl Into<String>) -> Self {
        Self {
            pool,
            queue: queue.into(),
            concurrency: 1,
            visibility_timeout: Duration::from_secs(30),
            poll_interval: Duration::from_millis(100),
            token: CancellationToken::new(),
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

    pub fn spawn<F, Fut>(self, handler: F) -> Vec<JoinHandle<()>>
    where
        F: Fn(Task) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send,
    {
        let mut handles = Vec::with_capacity(self.concurrency);

        for worker_id in 0..self.concurrency {
            let pool = self.pool.clone();
            let queue = self.queue.clone();
            let handler = handler.clone();
            let visibility_timeout = self.visibility_timeout;
            let poll_interval = self.poll_interval;
            let token = self.token.clone();

            let handle = tokio::spawn(async move {
                loop {
                    if token.is_cancelled() {
                        debug!(worker_id, queue = %queue, "worker shutting down");
                        return;
                    }

                    match claim(&pool, &queue, visibility_timeout).await {
                        Ok(Some(task)) => {
                            let task_id = task.id;
                            let max_retries = task.attempts;
                            let attempts = task.attempts;

                            debug!(worker_id, task_id, queue = %queue, "claimed task");

                            match handler(task).await {
                                Ok(()) => {
                                    if let Err(e) = complete(&pool, task_id).await {
                                        error!(task_id, error = %e, "failed to mark task complete");
                                    }
                                }
                                Err(e) => {
                                    warn!(task_id, error = %e, "task handler failed");
                                    if let Err(e) = fail(&pool, task_id, max_retries, attempts).await {
                                        error!(task_id, error = %e, "failed to mark task failed");
                                    }
                                }
                            }
                        }
                        Ok(None) => {
                            tokio::select! {
                                _ = tokio::time::sleep(poll_interval) => {}
                                _ = token.cancelled() => {
                                    debug!(worker_id, queue = %queue, "worker shutting down");
                                    return;
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
