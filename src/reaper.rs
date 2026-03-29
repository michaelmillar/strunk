use std::time::Duration;

use sqlx::PgPool;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

use crate::error::Result;

pub struct Reaper {
    pool: PgPool,
    retention_delivered: Duration,
    retention_dead: Duration,
    batch_size: i64,
    interval: Duration,
    token: CancellationToken,
}

impl Reaper {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            retention_delivered: Duration::from_secs(7 * 24 * 3600),
            retention_dead: Duration::from_secs(30 * 24 * 3600),
            batch_size: 10_000,
            interval: Duration::from_secs(300),
            token: CancellationToken::new(),
        }
    }

    pub fn retention_delivered(mut self, retention: Duration) -> Self {
        self.retention_delivered = retention;
        self
    }

    pub fn retention_dead(mut self, retention: Duration) -> Self {
        self.retention_dead = retention;
        self
    }

    pub fn batch_size(mut self, size: i64) -> Self {
        self.batch_size = size;
        self
    }

    pub fn interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    pub fn cancellation_token(mut self, token: CancellationToken) -> Self {
        self.token = token;
        self
    }

    async fn reap_once(&self) -> Result<u64> {
        let mut total = 0u64;

        loop {
            let result = sqlx::query(
                r#"
                WITH batch AS (
                    SELECT id FROM strunk_outbox
                    WHERE (status = 'delivered' AND delivered_at < now() - make_interval(secs => $1::double precision))
                       OR (status = 'dead' AND delivered_at < now() - make_interval(secs => $2::double precision))
                    LIMIT $3
                    FOR UPDATE SKIP LOCKED
                )
                DELETE FROM strunk_outbox
                WHERE id IN (SELECT id FROM batch)
                "#,
            )
            .bind(self.retention_delivered.as_secs_f64())
            .bind(self.retention_dead.as_secs_f64())
            .bind(self.batch_size)
            .execute(&self.pool)
            .await?;

            let deleted = result.rows_affected();
            total += deleted;

            if (deleted as i64) < self.batch_size {
                break;
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        sqlx::query(
            "DELETE FROM strunk_inbox WHERE processed_at < now() - make_interval(secs => $1::double precision)",
        )
        .bind(self.retention_delivered.as_secs_f64())
        .execute(&self.pool)
        .await?;

        Ok(total)
    }

    pub fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                if self.token.is_cancelled() {
                    debug!("reaper shutting down");
                    return;
                }

                match self.reap_once().await {
                    Ok(0) => {
                        debug!("reaper found nothing to clean");
                    }
                    Ok(count) => {
                        debug!(count, "reaper cleaned old rows");
                    }
                    Err(e) => {
                        error!(error = %e, "reaper failed");
                    }
                }

                tokio::select! {
                    _ = tokio::time::sleep(self.interval) => {}
                    _ = self.token.cancelled() => {
                        debug!("reaper shutting down");
                        return;
                    }
                }
            }
        })
    }
}
