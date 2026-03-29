use std::time::Duration;

use sqlx::PgPool;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

use crate::error::Result;
use crate::types::OutboxRow;

pub struct Relay {
    pool: PgPool,
    poll_interval: Duration,
    batch_size: i64,
    token: CancellationToken,
}

impl Relay {
    pub fn new(pool: PgPool, poll_interval: Duration, batch_size: i64) -> Self {
        Self {
            pool,
            poll_interval,
            batch_size,
            token: CancellationToken::new(),
        }
    }

    pub fn cancellation_token(mut self, token: CancellationToken) -> Self {
        self.token = token;
        self
    }

    pub async fn poll_pending(&self) -> Result<Vec<OutboxRow>> {
        let rows = sqlx::query_as::<_, OutboxRow>(
            r#"
            UPDATE strunk_outbox
            SET status = 'delivered', delivered_at = now()
            WHERE id IN (
                SELECT id FROM strunk_outbox
                WHERE status = 'pending'
                AND kind = 'event'
                AND visible_at <= now()
                ORDER BY priority DESC, id
                LIMIT $1
                FOR UPDATE SKIP LOCKED
            )
            RETURNING *
            "#,
        )
        .bind(self.batch_size)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    pub fn spawn<F, Fut>(self, handler: F) -> JoinHandle<()>
    where
        F: Fn(OutboxRow) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send,
    {
        tokio::spawn(async move {
            loop {
                if self.token.is_cancelled() {
                    debug!("relay shutting down");
                    return;
                }

                match self.poll_pending().await {
                    Ok(rows) => {
                        let count = rows.len();
                        if count > 0 {
                            debug!(count, "relay delivered event messages");
                        }
                        for row in rows {
                            handler(row).await;
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "relay poll failed");
                    }
                }

                tokio::select! {
                    _ = tokio::time::sleep(self.poll_interval) => {}
                    _ = self.token.cancelled() => {
                        debug!("relay shutting down");
                        return;
                    }
                }
            }
        })
    }
}

pub async fn mark_delivered(pool: &PgPool, id: i64) -> Result<()> {
    sqlx::query(
        "UPDATE strunk_outbox SET status = 'delivered', delivered_at = now() WHERE id = $1",
    )
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn mark_failed(pool: &PgPool, id: i64, max_retries: i32, attempts: i32) -> Result<()> {
    if attempts >= max_retries {
        sqlx::query("UPDATE strunk_outbox SET status = 'dead', delivered_at = now() WHERE id = $1")
            .bind(id)
            .execute(pool)
            .await?;
    } else {
        let backoff_secs = 2_i64.pow(attempts as u32);
        let jitter = rand::random::<f64>() * backoff_secs as f64;
        let delay_secs = backoff_secs + jitter as i64;

        sqlx::query(
            r#"
            UPDATE strunk_outbox
            SET status = 'pending',
                visible_at = now() + make_interval(secs => $2::double precision)
            WHERE id = $1
            "#,
        )
        .bind(id)
        .bind(delay_secs as f64)
        .execute(pool)
        .await?;
    }
    Ok(())
}
