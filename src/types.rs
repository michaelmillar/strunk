use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "text", rename_all = "lowercase")]
pub enum MessageKind {
    Task,
    Event,
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
    pub dedup_key: Option<String>,
    pub payload: serde_json::Value,
    pub metadata: serde_json::Value,
    pub status: MessageStatus,
    pub attempts: i32,
    pub max_retries: i32,
    pub priority: i32,
    pub progress: i16,
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
    pub max_retries: i32,
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
            max_retries: row.max_retries,
            created_at: row.created_at,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskResult {
    pub task_id: i64,
    pub queue: String,
    pub result: serde_json::Value,
    pub completed_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct StateEvent {
    pub id: i64,
    pub entity_type: String,
    pub entity_id: String,
    pub state: serde_json::Value,
    pub diff: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct TypedTask<T> {
    pub id: i64,
    pub queue: String,
    pub data: T,
    pub metadata: serde_json::Value,
    pub attempts: i32,
    pub max_retries: i32,
    pub created_at: DateTime<Utc>,
}

impl<T: DeserializeOwned> TypedTask<T> {
    pub fn try_from_task(task: Task) -> std::result::Result<Self, serde_json::Error> {
        let data = serde_json::from_value(task.payload)?;
        Ok(Self {
            id: task.id,
            queue: task.queue,
            data,
            metadata: task.metadata,
            attempts: task.attempts,
            max_retries: task.max_retries,
            created_at: task.created_at,
        })
    }
}

#[derive(Debug, Clone)]
pub struct TypedStateEvent<T> {
    pub id: i64,
    pub entity_type: String,
    pub entity_id: String,
    pub data: T,
    pub diff: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
}

impl<T: DeserializeOwned> TypedStateEvent<T> {
    pub fn try_from_event(event: StateEvent) -> std::result::Result<Self, serde_json::Error> {
        let data = serde_json::from_value(event.state)?;
        Ok(Self {
            id: event.id,
            entity_type: event.entity_type,
            entity_id: event.entity_id,
            data,
            diff: event.diff,
            created_at: event.created_at,
        })
    }
}

impl From<OutboxRow> for StateEvent {
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
