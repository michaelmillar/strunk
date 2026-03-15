use chrono::{DateTime, Utc};
use sqlx::PgPool;

use crate::error::Result;

#[derive(Debug, Clone)]
pub struct QueueStats {
    pub queue: String,
    pub pending: i64,
    pub claimed: i64,
    pub dead: i64,
    pub delivered: i64,
    pub oldest_pending: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct SubscriberStats {
    pub id: String,
    pub entity_type: String,
    pub last_seen_id: i64,
    pub latest_outbox_id: i64,
    pub lag: i64,
}

#[derive(Debug, Clone)]
pub struct OverallStats {
    pub total_pending: i64,
    pub total_claimed: i64,
    pub total_delivered: i64,
    pub total_dead: i64,
    pub table_size: i64,
}

pub async fn queue_stats(pool: &PgPool, queue: &str) -> Result<QueueStats> {
    let row = sqlx::query_as::<_, (i64, i64, i64, i64, Option<DateTime<Utc>>)>(
        r#"
        SELECT
            count(*) FILTER (WHERE status = 'pending') as pending,
            count(*) FILTER (WHERE status = 'claimed') as claimed,
            count(*) FILTER (WHERE status = 'dead') as dead,
            count(*) FILTER (WHERE status = 'delivered') as delivered,
            min(created_at) FILTER (WHERE status = 'pending') as oldest_pending
        FROM strunk_outbox
        WHERE key = $1 AND kind = 'task'
        "#,
    )
    .bind(queue)
    .fetch_one(pool)
    .await?;

    Ok(QueueStats {
        queue: queue.to_string(),
        pending: row.0,
        claimed: row.1,
        dead: row.2,
        delivered: row.3,
        oldest_pending: row.4,
    })
}

pub async fn subscriber_stats(pool: &PgPool, subscriber_id: &str) -> Result<Option<SubscriberStats>> {
    let sub = sqlx::query_as::<_, (String, String, i64)>(
        "SELECT id, entity_type, last_seen_id FROM strunk_subscribers WHERE id = $1",
    )
    .bind(subscriber_id)
    .fetch_optional(pool)
    .await?;

    let Some((id, entity_type, last_seen_id)) = sub else {
        return Ok(None);
    };

    let key_prefix = format!("{}:", entity_type);
    let latest_id = sqlx::query_scalar::<_, Option<i64>>(
        r#"
        SELECT max(id) FROM strunk_outbox
        WHERE kind = 'change' AND key LIKE $1 || '%'
        "#,
    )
    .bind(&key_prefix)
    .fetch_one(pool)
    .await?
    .unwrap_or(0);

    Ok(Some(SubscriberStats {
        id,
        entity_type,
        last_seen_id,
        latest_outbox_id: latest_id,
        lag: latest_id - last_seen_id,
    }))
}

pub async fn overall_stats(pool: &PgPool) -> Result<OverallStats> {
    let row = sqlx::query_as::<_, (i64, i64, i64, i64)>(
        r#"
        SELECT
            count(*) FILTER (WHERE status = 'pending') as pending,
            count(*) FILTER (WHERE status = 'claimed') as claimed,
            count(*) FILTER (WHERE status = 'delivered') as delivered,
            count(*) FILTER (WHERE status = 'dead') as dead
        FROM strunk_outbox
        "#,
    )
    .fetch_one(pool)
    .await?;

    let table_size = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM strunk_outbox",
    )
    .fetch_one(pool)
    .await?;

    Ok(OverallStats {
        total_pending: row.0,
        total_claimed: row.1,
        total_delivered: row.2,
        total_dead: row.3,
        table_size,
    })
}
