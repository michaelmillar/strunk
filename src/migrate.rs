use sqlx::PgPool;

use crate::error::Result;

const CREATE_OUTBOX: &str = r#"
CREATE TABLE IF NOT EXISTS strunk_outbox (
    id           BIGSERIAL PRIMARY KEY,
    kind         TEXT NOT NULL,
    key          TEXT NOT NULL,
    payload      JSONB NOT NULL,
    metadata     JSONB NOT NULL DEFAULT '{}',
    status       TEXT NOT NULL DEFAULT 'pending',
    attempts     INT NOT NULL DEFAULT 0,
    max_retries  INT NOT NULL DEFAULT 3,
    priority     INT NOT NULL DEFAULT 0,
    visible_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at TIMESTAMPTZ
)
"#;

const CREATE_INDEX_POLL: &str = r#"
CREATE INDEX IF NOT EXISTS idx_strunk_outbox_poll
    ON strunk_outbox (visible_at, priority DESC, id)
    WHERE status = 'pending'
"#;

const CREATE_INDEX_CLAIM: &str = r#"
CREATE INDEX IF NOT EXISTS idx_strunk_outbox_claim
    ON strunk_outbox (key, visible_at, priority DESC, id)
    WHERE status = 'pending' AND kind = 'task'
"#;

const CREATE_INDEX_REAPER: &str = r#"
CREATE INDEX IF NOT EXISTS idx_strunk_outbox_reaper
    ON strunk_outbox (delivered_at)
    WHERE status IN ('delivered', 'dead')
"#;

const CREATE_INDEX_DEAD: &str = r#"
CREATE INDEX IF NOT EXISTS idx_strunk_outbox_dead
    ON strunk_outbox (key, created_at DESC)
    WHERE status = 'dead'
"#;

const CREATE_SUBSCRIBERS: &str = r#"
CREATE TABLE IF NOT EXISTS strunk_subscribers (
    id              TEXT PRIMARY KEY,
    entity_type     TEXT NOT NULL,
    last_seen_id    BIGINT NOT NULL DEFAULT 0,
    schema_version  TEXT NOT NULL DEFAULT '1.0',
    registered_at   TIMESTAMPTZ NOT NULL DEFAULT now()
)
"#;

const CREATE_SNAPSHOTS: &str = r#"
CREATE TABLE IF NOT EXISTS strunk_snapshots (
    entity_type  TEXT NOT NULL,
    entity_id    TEXT NOT NULL,
    state        JSONB NOT NULL,
    version      TEXT NOT NULL DEFAULT '1.0',
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (entity_type, entity_id)
)
"#;

const CREATE_SCHEMAS: &str = r#"
CREATE TABLE IF NOT EXISTS strunk_schemas (
    entity_type  TEXT NOT NULL,
    version      TEXT NOT NULL,
    schema       JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (entity_type, version)
)
"#;

pub async fn migrate(pool: &PgPool) -> Result<()> {
    sqlx::query(CREATE_OUTBOX).execute(pool).await?;
    sqlx::query(CREATE_INDEX_POLL).execute(pool).await?;
    sqlx::query(CREATE_INDEX_CLAIM).execute(pool).await?;
    sqlx::query(CREATE_INDEX_REAPER).execute(pool).await?;
    sqlx::query(CREATE_INDEX_DEAD).execute(pool).await?;
    sqlx::query(CREATE_SUBSCRIBERS).execute(pool).await?;
    sqlx::query(CREATE_SNAPSHOTS).execute(pool).await?;
    sqlx::query(CREATE_SCHEMAS).execute(pool).await?;
    Ok(())
}
