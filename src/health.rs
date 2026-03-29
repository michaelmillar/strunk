use std::time::Duration;

use serde::Serialize;
use sqlx::PgPool;

use crate::error::Result;

#[derive(Debug, Clone, Serialize)]
pub struct HealthReport {
    pub healthy: bool,
    pub pending: i64,
    pub oldest_pending_age_secs: Option<i64>,
}

pub async fn check(pool: &PgPool, max_pending_age: Duration) -> Result<HealthReport> {
    let row = sqlx::query_as::<_, (i64, Option<f64>)>(
        r#"
        SELECT
            count(*) FILTER (WHERE status = 'pending'),
            EXTRACT(EPOCH FROM (now() - min(created_at) FILTER (WHERE status = 'pending')))
        FROM strunk_outbox
        "#,
    )
    .fetch_one(pool)
    .await?;

    let pending = row.0;
    let oldest_age_secs = row.1.map(|s| s as i64);
    let healthy = match oldest_age_secs {
        Some(age) => age < max_pending_age.as_secs() as i64,
        None => true,
    };

    Ok(HealthReport {
        healthy,
        pending,
        oldest_pending_age_secs: oldest_age_secs,
    })
}
