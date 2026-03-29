use sqlx::PgPool;

use crate::error::Result;

const CREATE_OUTBOX: &str = r#"
CREATE TABLE IF NOT EXISTS strunk_outbox (
    id           BIGSERIAL PRIMARY KEY,
    kind         TEXT NOT NULL,
    key          TEXT NOT NULL,
    dedup_key    TEXT,
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

const ADD_DEDUP_COLUMN: &str = r#"
ALTER TABLE strunk_outbox ADD COLUMN IF NOT EXISTS dedup_key TEXT
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

const CREATE_INDEX_DEDUP: &str = r#"
CREATE UNIQUE INDEX IF NOT EXISTS idx_strunk_outbox_dedup
    ON strunk_outbox (dedup_key)
    WHERE dedup_key IS NOT NULL AND status IN ('pending', 'claimed')
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

const ADD_PROGRESS_COLUMN: &str = r#"
ALTER TABLE strunk_outbox ADD COLUMN IF NOT EXISTS progress SMALLINT NOT NULL DEFAULT 0
"#;

const CREATE_RESULTS: &str = r#"
CREATE TABLE IF NOT EXISTS strunk_results (
    task_id     BIGINT PRIMARY KEY,
    queue       TEXT NOT NULL,
    result      JSONB NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"#;

const CREATE_SCHEDULES: &str = r#"
CREATE TABLE IF NOT EXISTS strunk_schedules (
    id           TEXT PRIMARY KEY,
    queue        TEXT NOT NULL,
    payload      JSONB NOT NULL DEFAULT '{}',
    cron         TEXT NOT NULL,
    max_retries  INT NOT NULL DEFAULT 3,
    priority     INT NOT NULL DEFAULT 0,
    enabled      BOOLEAN NOT NULL DEFAULT true,
    last_fired   TIMESTAMPTZ,
    next_fire    TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
)
"#;

const CREATE_INDEX_SCHEDULES: &str = r#"
CREATE INDEX IF NOT EXISTS idx_strunk_schedules_next
    ON strunk_schedules (next_fire)
    WHERE enabled = true
"#;

const CREATE_INBOX: &str = r#"
CREATE TABLE IF NOT EXISTS strunk_inbox (
    consumer_id  TEXT NOT NULL,
    message_id   BIGINT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_id, message_id)
)
"#;

const CREATE_INDEX_INBOX: &str = r#"
CREATE INDEX IF NOT EXISTS idx_strunk_inbox_age
    ON strunk_inbox (processed_at)
"#;

const CREATE_NOTIFY_FUNCTION: &str = r#"
CREATE OR REPLACE FUNCTION strunk_notify() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('strunk', NEW.kind || ':' || NEW.key);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql
"#;

const CREATE_NOTIFY_TRIGGER: &str = r#"
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'strunk_outbox_notify') THEN
        CREATE TRIGGER strunk_outbox_notify
            AFTER INSERT ON strunk_outbox
            FOR EACH ROW EXECUTE FUNCTION strunk_notify();
    END IF;
END $$
"#;

const RENAME_KIND_CHANGE_TO_EVENT: &str = r#"
UPDATE strunk_outbox SET kind = 'event' WHERE kind = 'change'
"#;

pub async fn migrate(pool: &PgPool) -> Result<()> {
    sqlx::query(CREATE_OUTBOX).execute(pool).await?;
    sqlx::query(ADD_DEDUP_COLUMN).execute(pool).await?;
    sqlx::query(ADD_PROGRESS_COLUMN).execute(pool).await?;
    sqlx::query(RENAME_KIND_CHANGE_TO_EVENT).execute(pool).await?;
    sqlx::query(CREATE_INDEX_POLL).execute(pool).await?;
    sqlx::query(CREATE_INDEX_CLAIM).execute(pool).await?;
    sqlx::query(CREATE_INDEX_REAPER).execute(pool).await?;
    sqlx::query(CREATE_INDEX_DEAD).execute(pool).await?;
    sqlx::query(CREATE_INDEX_DEDUP).execute(pool).await?;
    sqlx::query(CREATE_SUBSCRIBERS).execute(pool).await?;
    sqlx::query(CREATE_SNAPSHOTS).execute(pool).await?;
    sqlx::query(CREATE_SCHEMAS).execute(pool).await?;
    sqlx::query(CREATE_RESULTS).execute(pool).await?;
    sqlx::query(CREATE_SCHEDULES).execute(pool).await?;
    sqlx::query(CREATE_INDEX_SCHEDULES).execute(pool).await?;
    sqlx::query(CREATE_INBOX).execute(pool).await?;
    sqlx::query(CREATE_INDEX_INBOX).execute(pool).await?;
    sqlx::query(CREATE_NOTIFY_FUNCTION).execute(pool).await?;
    sqlx::query(CREATE_NOTIFY_TRIGGER).execute(pool).await?;
    Ok(())
}
