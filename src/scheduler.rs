use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::PgPool;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

use crate::error::Result;

#[derive(Debug, Clone, Serialize)]
pub struct Schedule {
    pub id: String,
    pub queue: String,
    pub payload: serde_json::Value,
    pub cron: String,
    pub max_retries: i32,
    pub priority: i32,
    pub enabled: bool,
    pub last_fired: Option<DateTime<Utc>>,
    pub next_fire: DateTime<Utc>,
}

pub struct ScheduleBuilder {
    pool: PgPool,
    id: String,
    queue: String,
    payload: serde_json::Value,
    cron: String,
    max_retries: i32,
    priority: i32,
}

impl ScheduleBuilder {
    pub fn new(pool: PgPool, id: impl Into<String>, queue: impl Into<String>, cron: impl Into<String>) -> Self {
        Self {
            pool,
            id: id.into(),
            queue: queue.into(),
            payload: serde_json::json!({}),
            cron: cron.into(),
            max_retries: 3,
            priority: 0,
        }
    }

    pub fn payload(mut self, payload: serde_json::Value) -> Self {
        self.payload = payload;
        self
    }

    pub fn max_retries(mut self, max_retries: i32) -> Self {
        self.max_retries = max_retries;
        self
    }

    pub fn priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    pub async fn register(self) -> Result<()> {
        let next = next_fire_from_cron(&self.cron)?;

        sqlx::query(
            r#"
            INSERT INTO strunk_schedules (id, queue, payload, cron, max_retries, priority, next_fire)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (id) DO UPDATE
            SET queue = $2, payload = $3, cron = $4, max_retries = $5, priority = $6,
                enabled = true
            "#,
        )
        .bind(&self.id)
        .bind(&self.queue)
        .bind(&self.payload)
        .bind(&self.cron)
        .bind(self.max_retries)
        .bind(self.priority)
        .bind(next)
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}

pub async fn disable(pool: &PgPool, id: &str) -> Result<()> {
    sqlx::query("UPDATE strunk_schedules SET enabled = false WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn enable(pool: &PgPool, id: &str) -> Result<()> {
    sqlx::query("UPDATE strunk_schedules SET enabled = true WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn list(pool: &PgPool) -> Result<Vec<Schedule>> {
    let rows = sqlx::query_as::<_, (String, String, serde_json::Value, String, i32, i32, bool, Option<DateTime<Utc>>, DateTime<Utc>)>(
        "SELECT id, queue, payload, cron, max_retries, priority, enabled, last_fired, next_fire FROM strunk_schedules ORDER BY next_fire",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().map(|(id, queue, payload, cron, max_retries, priority, enabled, last_fired, next_fire)| Schedule {
        id, queue, payload, cron, max_retries, priority, enabled, last_fired, next_fire,
    }).collect())
}

pub async fn remove(pool: &PgPool, id: &str) -> Result<()> {
    sqlx::query("DELETE FROM strunk_schedules WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

async fn tick(pool: &PgPool) -> Result<u64> {
    let due = sqlx::query_as::<_, (String, String, serde_json::Value, String, i32, i32)>(
        r#"
        SELECT id, queue, payload, cron, max_retries, priority
        FROM strunk_schedules
        WHERE enabled = true AND next_fire <= now()
        ORDER BY next_fire
        LIMIT 100
        FOR UPDATE SKIP LOCKED
        "#,
    )
    .fetch_all(pool)
    .await?;

    let mut fired = 0u64;

    for (sched_id, queue, payload, cron, max_retries, priority) in due {
        let dedup = format!("sched:{}:{}", sched_id, Utc::now().format("%Y%m%d%H%M"));

        let insert_result = sqlx::query(
            r#"
            INSERT INTO strunk_outbox (kind, key, dedup_key, payload, priority, max_retries)
            VALUES ('task', $1, $2, $3, $4, $5)
            ON CONFLICT DO NOTHING
            "#,
        )
        .bind(&queue)
        .bind(&dedup)
        .bind(&payload)
        .bind(priority)
        .bind(max_retries)
        .execute(pool)
        .await?;

        if insert_result.rows_affected() > 0 {
            fired += 1;
        }

        let next = next_fire_from_cron(&cron).unwrap_or_else(|_| Utc::now() + chrono::Duration::hours(1));

        sqlx::query(
            "UPDATE strunk_schedules SET last_fired = now(), next_fire = $2 WHERE id = $1",
        )
        .bind(&sched_id)
        .bind(next)
        .execute(pool)
        .await?;
    }

    Ok(fired)
}

pub struct Scheduler {
    pool: PgPool,
    interval: Duration,
    token: CancellationToken,
}

impl Scheduler {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            interval: Duration::from_secs(30),
            token: CancellationToken::new(),
        }
    }

    pub fn interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    pub fn cancellation_token(mut self, token: CancellationToken) -> Self {
        self.token = token;
        self
    }

    pub fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                if self.token.is_cancelled() {
                    debug!("scheduler shutting down");
                    return;
                }

                match tick(&self.pool).await {
                    Ok(0) => {}
                    Ok(n) => debug!(fired = n, "scheduler fired tasks"),
                    Err(e) => error!(error = %e, "scheduler tick failed"),
                }

                tokio::select! {
                    _ = tokio::time::sleep(self.interval) => {}
                    _ = self.token.cancelled() => {
                        debug!("scheduler shutting down");
                        return;
                    }
                }
            }
        })
    }
}

fn next_fire_from_cron(cron_expr: &str) -> Result<DateTime<Utc>> {
    let parts: Vec<&str> = cron_expr.split_whitespace().collect();
    if parts.len() < 2 {
        return Err(crate::error::StrunkError::Config(
            format!("invalid cron expression: '{}'", cron_expr),
        ));
    }

    let now = Utc::now();
    let interval = parse_simple_interval(cron_expr)?;
    Ok(now + interval)
}

fn parse_simple_interval(expr: &str) -> Result<chrono::Duration> {
    let trimmed = expr.trim();

    if trimmed.starts_with("every ") || trimmed.starts_with("@every ") {
        let duration_part = trimmed
            .strip_prefix("every ")
            .or_else(|| trimmed.strip_prefix("@every "))
            .unwrap_or(trimmed);
        return parse_duration_string(duration_part);
    }

    if trimmed.starts_with('@') {
        return match trimmed {
            "@hourly" => Ok(chrono::Duration::hours(1)),
            "@daily" => Ok(chrono::Duration::days(1)),
            "@weekly" => Ok(chrono::Duration::weeks(1)),
            "@monthly" => Ok(chrono::Duration::days(30)),
            _ => Err(crate::error::StrunkError::Config(
                format!("unsupported schedule shorthand: '{}'", trimmed),
            )),
        };
    }

    parse_duration_string(trimmed)
}

fn parse_duration_string(s: &str) -> Result<chrono::Duration> {
    let s = s.trim();

    if let Some(n) = s.strip_suffix('s') {
        let secs: i64 = n.trim().parse().map_err(|_| crate::error::StrunkError::Config(format!("invalid duration: '{}'", s)))?;
        return Ok(chrono::Duration::seconds(secs));
    }
    if let Some(n) = s.strip_suffix('m') {
        let mins: i64 = n.trim().parse().map_err(|_| crate::error::StrunkError::Config(format!("invalid duration: '{}'", s)))?;
        return Ok(chrono::Duration::minutes(mins));
    }
    if let Some(n) = s.strip_suffix('h') {
        let hours: i64 = n.trim().parse().map_err(|_| crate::error::StrunkError::Config(format!("invalid duration: '{}'", s)))?;
        return Ok(chrono::Duration::hours(hours));
    }
    if let Some(n) = s.strip_suffix('d') {
        let days: i64 = n.trim().parse().map_err(|_| crate::error::StrunkError::Config(format!("invalid duration: '{}'", s)))?;
        return Ok(chrono::Duration::days(days));
    }

    if let Ok(secs) = s.parse::<i64>() {
        return Ok(chrono::Duration::seconds(secs));
    }

    Err(crate::error::StrunkError::Config(
        format!("cannot parse schedule interval: '{}'. Use formats like '30s', '5m', '1h', '1d', '@hourly', '@daily', 'every 5m'", s),
    ))
}
