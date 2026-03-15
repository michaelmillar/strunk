use std::collections::HashMap;

use sqlx::PgPool;
use tracing::debug;

use crate::error::{Result, StrunkError};

#[derive(Debug, Clone)]
struct FieldDef {
    field_type: String,
    required: bool,
}

#[derive(Debug, Clone)]
struct SchemaVersion {
    fields: HashMap<String, FieldDef>,
}

impl SchemaVersion {
    fn from_json(schema: &serde_json::Value) -> Option<Self> {
        let properties = schema.get("properties")?.as_object()?;
        let required: Vec<String> = schema
            .get("required")
            .and_then(|r| r.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let mut fields = HashMap::new();
        for (name, def) in properties {
            let field_type = def
                .get("type")
                .and_then(|t| t.as_str())
                .unwrap_or("any")
                .to_string();
            fields.insert(
                name.clone(),
                FieldDef {
                    field_type,
                    required: required.contains(name),
                },
            );
        }

        Some(Self { fields })
    }

    fn check_backward_compatible(&self, new: &SchemaVersion) -> std::result::Result<(), String> {
        for (name, old_field) in &self.fields {
            match new.fields.get(name) {
                None => {
                    if old_field.required {
                        return Err(format!(
                            "required field '{}' was removed, breaking backward compatibility",
                            name
                        ));
                    }
                }
                Some(new_field) => {
                    if old_field.field_type != new_field.field_type {
                        return Err(format!(
                            "field '{}' changed type from '{}' to '{}', breaking backward compatibility",
                            name, old_field.field_type, new_field.field_type
                        ));
                    }
                }
            }
        }

        for (name, new_field) in &new.fields {
            if new_field.required && !self.fields.contains_key(name) {
                return Err(format!(
                    "new field '{}' is required but did not exist in the previous version, breaking backward compatibility",
                    name
                ));
            }
        }

        Ok(())
    }
}

pub struct SchemaRegistry {
    schemas: HashMap<String, HashMap<String, SchemaVersion>>,
}

impl SchemaRegistry {
    pub fn new() -> Self {
        Self {
            schemas: HashMap::new(),
        }
    }

    pub fn register(
        &mut self,
        entity_type: &str,
        version: &str,
        schema: &serde_json::Value,
    ) -> Result<()> {
        let parsed = SchemaVersion::from_json(schema).ok_or_else(|| {
            StrunkError::Config(
                "schema must have 'properties' object with typed fields".to_string(),
            )
        })?;

        let latest_key = self
            .schemas
            .get(entity_type)
            .and_then(|v| v.keys().max_by(|a, b| version_cmp(a, b)).cloned());

        if let Some(ref key) = latest_key {
            if let Some(prev) = self.schemas.get(entity_type).and_then(|v| v.get(key)) {
                prev.check_backward_compatible(&parsed)
                    .map_err(StrunkError::Config)?;
            }
        }

        debug!(entity_type, version, "registered schema");
        self.schemas
            .entry(entity_type.to_string())
            .or_default()
            .insert(version.to_string(), parsed);
        Ok(())
    }

    pub fn validate(
        &self,
        entity_type: &str,
        version: &str,
        payload: &serde_json::Value,
    ) -> Result<()> {
        let versions = self.schemas.get(entity_type).ok_or_else(|| {
            StrunkError::Config(format!("no schemas registered for entity type '{}'", entity_type))
        })?;

        let schema = versions.get(version).ok_or_else(|| {
            StrunkError::Config(format!(
                "no schema version '{}' registered for entity type '{}'",
                version, entity_type
            ))
        })?;

        let obj = payload.as_object().ok_or_else(|| {
            StrunkError::Config("payload must be a JSON object".to_string())
        })?;

        for (name, field) in &schema.fields {
            if field.required && !obj.contains_key(name) {
                return Err(StrunkError::Config(format!(
                    "required field '{}' missing from payload",
                    name
                )));
            }

            if let Some(value) = obj.get(name) {
                let actual_type = json_type_name(value);
                if actual_type != "null" && actual_type != field.field_type && field.field_type != "any" {
                    return Err(StrunkError::Config(format!(
                        "field '{}' expected type '{}' but got '{}'",
                        name, field.field_type, actual_type
                    )));
                }
            }
        }

        Ok(())
    }

    pub async fn persist(&self, pool: &PgPool, entity_type: &str) -> Result<()> {
        if let Some(versions) = self.schemas.get(entity_type) {
            for (version, schema) in versions {
                let schema_json = schema_to_json(schema);
                sqlx::query(
                    r#"
                    INSERT INTO strunk_schemas (entity_type, version, schema)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (entity_type, version) DO NOTHING
                    "#,
                )
                .bind(entity_type)
                .bind(version)
                .bind(&schema_json)
                .execute(pool)
                .await?;
            }
        }
        Ok(())
    }
}

fn json_type_name(value: &serde_json::Value) -> &str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(n) => {
            if n.is_i64() || n.is_u64() {
                "integer"
            } else {
                "number"
            }
        }
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

fn version_cmp(a: &str, b: &str) -> std::cmp::Ordering {
    let a_parts: Vec<u64> = a.split('.').filter_map(|p| p.parse().ok()).collect();
    let b_parts: Vec<u64> = b.split('.').filter_map(|p| p.parse().ok()).collect();
    a_parts.cmp(&b_parts)
}

fn schema_to_json(schema: &SchemaVersion) -> serde_json::Value {
    let mut properties = serde_json::Map::new();
    let mut required = Vec::new();

    for (name, field) in &schema.fields {
        properties.insert(
            name.clone(),
            serde_json::json!({ "type": field.field_type }),
        );
        if field.required {
            required.push(serde_json::Value::String(name.clone()));
        }
    }

    serde_json::json!({
        "type": "object",
        "properties": properties,
        "required": required
    })
}
