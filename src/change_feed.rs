use std::future::Future;
use std::time::Duration;

use sqlx::{PgPool, Postgres, Transaction};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

use crate::error::Result;
use crate::schema::SchemaRegistry;
use crate::types::{Change, OutboxRow};

pub struct ChangePublish<'a> {
    tx: &'a mut Transaction<'static, Postgres>,
    entity_type: String,
    entity_id: String,
    state: serde_json::Value,
    diff: Option<serde_json::Value>,
    schema_version: String,
    registry: Option<&'a SchemaRegistry>,
}

impl<'a> ChangePublish<'a> {
    pub fn new(
        tx: &'a mut Transaction<'static, Postgres>,
        entity_type: impl Into<String>,
        entity_id: impl Into<String>,
    ) -> Self {
        Self {
            tx,
            entity_type: entity_type.into(),
            entity_id: entity_id.into(),
            state: serde_json::Value::Null,
            diff: None,
            schema_version: "1.0".to_string(),
            registry: None,
        }
    }

    pub fn state(mut self, state: serde_json::Value) -> Self {
        self.state = state;
        self
    }

    pub fn diff(mut self, diff: serde_json::Value) -> Self {
        self.diff = Some(diff);
        self
    }

    pub fn schema_version(mut self, version: impl Into<String>) -> Self {
        self.schema_version = version.into();
        self
    }

    pub fn with_registry(mut self, registry: &'a SchemaRegistry) -> Self {
        self.registry = Some(registry);
        self
    }

    pub async fn publish(self) -> Result<i64> {
        if let Some(registry) = self.registry {
            registry.validate(&self.entity_type, &self.schema_version, &self.state)?;
        }

        let key = format!("{}:{}", self.entity_type, self.entity_id);
        let metadata = match self.diff {
            Some(diff) => serde_json::json!({
                "diff": diff,
                "schema_version": self.schema_version
            }),
            None => serde_json::json!({
                "schema_version": self.schema_version
            }),
        };

        let id = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO strunk_outbox (kind, key, payload, metadata)
            VALUES ('change', $1, $2, $3)
            RETURNING id
            "#,
        )
        .bind(&key)
        .bind(&self.state)
        .bind(&metadata)
        .fetch_one(&mut **self.tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO strunk_snapshots (entity_type, entity_id, state, version, updated_at)
            VALUES ($1, $2, $3, $4, now())
            ON CONFLICT (entity_type, entity_id)
            DO UPDATE SET state = $3, version = $4, updated_at = now()
            "#,
        )
        .bind(&self.entity_type)
        .bind(&self.entity_id)
        .bind(&self.state)
        .bind(&self.schema_version)
        .execute(&mut **self.tx)
        .await?;

        Ok(id)
    }
}

pub async fn snapshot(
    pool: &PgPool,
    entity_type: &str,
    entity_id: &str,
) -> Result<Option<serde_json::Value>> {
    let state = sqlx::query_scalar::<_, serde_json::Value>(
        "SELECT state FROM strunk_snapshots WHERE entity_type = $1 AND entity_id = $2",
    )
    .bind(entity_type)
    .bind(entity_id)
    .fetch_optional(pool)
    .await?;

    Ok(state)
}

pub struct Subscriber {
    pool: PgPool,
    id: String,
    entity_type: String,
    schema_version: String,
    poll_interval: Duration,
    batch_size: i64,
    token: CancellationToken,
}

impl Subscriber {
    pub fn new(pool: PgPool, id: impl Into<String>, entity_type: impl Into<String>) -> Self {
        Self {
            pool,
            id: id.into(),
            entity_type: entity_type.into(),
            schema_version: "1.0".to_string(),
            poll_interval: Duration::from_millis(100),
            batch_size: 100,
            token: CancellationToken::new(),
        }
    }

    pub fn schema_version(mut self, version: impl Into<String>) -> Self {
        self.schema_version = version.into();
        self
    }

    pub fn poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    pub fn batch_size(mut self, size: i64) -> Self {
        self.batch_size = size;
        self
    }

    pub fn cancellation_token(mut self, token: CancellationToken) -> Self {
        self.token = token;
        self
    }

    async fn register(&self) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO strunk_subscribers (id, entity_type, schema_version)
            VALUES ($1, $2, $3)
            ON CONFLICT (id) DO UPDATE
            SET schema_version = $3
            "#,
        )
        .bind(&self.id)
        .bind(&self.entity_type)
        .bind(&self.schema_version)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn last_seen_id(&self) -> Result<i64> {
        let id = sqlx::query_scalar::<_, i64>(
            "SELECT last_seen_id FROM strunk_subscribers WHERE id = $1",
        )
        .bind(&self.id)
        .fetch_one(&self.pool)
        .await?;
        Ok(id)
    }

    async fn advance_cursor(&self, last_id: i64) -> Result<()> {
        sqlx::query("UPDATE strunk_subscribers SET last_seen_id = $2 WHERE id = $1")
            .bind(&self.id)
            .bind(last_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn poll_changes(&self, after_id: i64) -> Result<Vec<OutboxRow>> {
        let key_prefix = format!("{}:", self.entity_type);

        let rows = sqlx::query_as::<_, OutboxRow>(
            r#"
            SELECT * FROM strunk_outbox
            WHERE kind = 'change'
            AND key LIKE $1 || '%'
            AND id > $2
            ORDER BY id
            LIMIT $3
            "#,
        )
        .bind(&key_prefix)
        .bind(after_id)
        .bind(self.batch_size)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    pub fn spawn<F, Fut>(self, handler: F) -> JoinHandle<()>
    where
        F: Fn(Change) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>>
            + Send,
    {
        tokio::spawn(async move {
            if let Err(e) = self.register().await {
                error!(error = %e, subscriber = %self.id, "failed to register subscriber");
                return;
            }

            loop {
                if self.token.is_cancelled() {
                    debug!(subscriber = %self.id, "subscriber shutting down");
                    return;
                }

                match self.last_seen_id().await {
                    Ok(cursor) => match self.poll_changes(cursor).await {
                        Ok(rows) => {
                            if !rows.is_empty() {
                                debug!(
                                    count = rows.len(),
                                    subscriber = %self.id,
                                    "processing changes"
                                );
                            }
                            for row in rows {
                                let row_id = row.id;
                                let change = Change::from(row);
                                match handler(change).await {
                                    Ok(()) => {
                                        if let Err(e) = self.advance_cursor(row_id).await {
                                            error!(
                                                error = %e,
                                                subscriber = %self.id,
                                                "failed to advance cursor"
                                            );
                                            break;
                                        }
                                    }
                                    Err(e) => {
                                        warn!(
                                            error = %e,
                                            subscriber = %self.id,
                                            row_id,
                                            "change handler failed, will retry"
                                        );
                                        break;
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            error!(error = %e, subscriber = %self.id, "poll failed");
                        }
                    },
                    Err(e) => {
                        error!(error = %e, subscriber = %self.id, "failed to read cursor");
                    }
                }

                tokio::select! {
                    _ = tokio::time::sleep(self.poll_interval) => {}
                    _ = self.token.cancelled() => {
                        debug!(subscriber = %self.id, "subscriber shutting down");
                        return;
                    }
                }
            }
        })
    }
}
