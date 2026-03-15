use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "text", rename_all = "lowercase")]
pub enum MessageKind {
    Task,
    Change,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "text", rename_all = "lowercase")]
pub enum MessageStatus {
    Pending,
    Claimed,
    Delivered,
    Failed,
    Dead,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct OutboxRow {
    pub id: i64,
    pub kind: MessageKind,
    pub key: String,
    pub payload: serde_json::Value,
    pub metadata: serde_json::Value,
    pub status: MessageStatus,
    pub attempts: i32,
    pub max_retries: i32,
    pub priority: i32,
    pub visible_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub delivered_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct Task {
    pub id: i64,
    pub queue: String,
    pub payload: serde_json::Value,
    pub metadata: serde_json::Value,
    pub attempts: i32,
    pub created_at: DateTime<Utc>,
}

impl From<OutboxRow> for Task {
    fn from(row: OutboxRow) -> Self {
        Self {
            id: row.id,
            queue: row.key,
            payload: row.payload,
            metadata: row.metadata,
            attempts: row.attempts,
            created_at: row.created_at,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Change {
    pub id: i64,
    pub entity_type: String,
    pub entity_id: String,
    pub state: serde_json::Value,
    pub diff: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
}

impl From<OutboxRow> for Change {
    fn from(row: OutboxRow) -> Self {
        let (entity_type, entity_id) = row
            .key
            .split_once(':')
            .map(|(t, i)| (t.to_string(), i.to_string()))
            .unwrap_or((row.key.clone(), String::new()));

        let diff = row.metadata.get("diff").cloned();

        Self {
            id: row.id,
            entity_type,
            entity_id,
            state: row.payload,
            diff,
            created_at: row.created_at,
        }
    }
}
