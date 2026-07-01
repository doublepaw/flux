// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Write command — validate JSON against Avro schema, encode, and append.

use std::io::Read;

use anyhow::{Context, Result, bail};
use apache_avro::Schema as AvroSchema;
use apache_avro::to_avro_datum;
use apache_avro::types::Value as AvroValue;
use flux_common::ids::{SchemaId, TopicId};
use flux_common::types::Record;
use flux_sdk::writer::{Writer, WriterConfig};

use crate::client::FluxClient;

/// Convert a serde_json::Value into an apache_avro::types::Value guided by the schema.
/// The naive From<serde_json::Value> produces Map for objects, but Avro expects Record.
fn json_to_avro(json: &serde_json::Value, schema: &AvroSchema) -> Result<AvroValue> {
    match schema {
        AvroSchema::Record(record_schema) => {
            let obj = json
                .as_object()
                .ok_or_else(|| anyhow::anyhow!("expected JSON object for record schema"))?;
            let known: std::collections::HashSet<&str> = record_schema
                .fields
                .iter()
                .map(|f| f.name.as_str())
                .collect();
            for key in obj.keys() {
                if !known.contains(key.as_str()) {
                    bail!("unknown field '{}' (schema has: {:?})", key, known);
                }
            }
            let mut fields = Vec::with_capacity(record_schema.fields.len());
            for field in &record_schema.fields {
                let value = match obj.get(&field.name) {
                    Some(v) => json_to_avro(v, &field.schema)?,
                    None => match &field.default {
                        Some(default) => json_to_avro(default, &field.schema)?,
                        None => bail!("missing required field '{}'", field.name),
                    },
                };
                fields.push((field.name.clone(), value));
            }
            Ok(AvroValue::Record(fields))
        }
        AvroSchema::Array(inner) => {
            let arr = json
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("expected JSON array"))?;
            let items: Result<Vec<_>> = arr.iter().map(|v| json_to_avro(v, &inner.items)).collect();
            Ok(AvroValue::Array(items?))
        }
        AvroSchema::Map(inner) => {
            let obj = json
                .as_object()
                .ok_or_else(|| anyhow::anyhow!("expected JSON object for map schema"))?;
            let mut map = std::collections::HashMap::new();
            for (k, v) in obj {
                map.insert(k.clone(), json_to_avro(v, &inner.types)?);
            }
            Ok(AvroValue::Map(map))
        }
        AvroSchema::Union(union_schema) => {
            // Try null first for JSON null
            if json.is_null() && union_schema.variants().contains(&AvroSchema::Null) {
                return Ok(AvroValue::Union(
                    union_schema
                        .variants()
                        .iter()
                        .position(|v| *v == AvroSchema::Null)
                        .unwrap() as u32,
                    Box::new(AvroValue::Null),
                ));
            }
            // Try each non-null variant
            for (i, variant) in union_schema.variants().iter().enumerate() {
                if *variant == AvroSchema::Null {
                    continue;
                }
                if let Ok(v) = json_to_avro(json, variant) {
                    return Ok(AvroValue::Union(i as u32, Box::new(v)));
                }
            }
            bail!("no matching union variant for value")
        }
        // Primitives — delegate to the From<serde_json::Value> impl
        _ => Ok(AvroValue::from(json.clone())),
    }
}

pub async fn run(
    ws_url: &str,
    api_key: Option<&str>,
    client: &FluxClient,
    topic_id: u32,
    schema_id: u32,
    key: Option<String>,
    value: Option<String>,
) -> Result<()> {
    let value = match value {
        Some(v) => v,
        None => {
            let mut buf = String::new();
            std::io::stdin()
                .read_to_string(&mut buf)
                .context("failed to read value from stdin")?;
            buf
        }
    };

    // Fetch schema and encode as Avro binary
    let schema_resp = client.get_schema(schema_id).await?;
    let schema_json = serde_json::to_string(&schema_resp.schema)?;
    let avro_schema = AvroSchema::parse_str(&schema_json).context("failed to parse Avro schema")?;

    let json_value: serde_json::Value =
        serde_json::from_str(&value).context("value is not valid JSON")?;
    let avro_value = json_to_avro(&json_value, &avro_schema)?;
    let encoded = to_avro_datum(&avro_schema, avro_value).context("schema validation failed")?;

    let config = WriterConfig {
        url: ws_url.to_string(),
        api_key: api_key.map(str::to_string),
        ..Default::default()
    };
    let writer = Writer::connect_with_config(config).await?;

    let record = match key {
        Some(k) => Record::with_key(k, encoded),
        None => Record::new(encoded),
    };

    let ack = writer
        .append(TopicId(topic_id), SchemaId(schema_id), vec![record])
        .await?;

    println!(
        "Appended to topic {} (offsets {}-{})",
        ack.topic_id.0, ack.start_offset.0, ack.end_offset.0,
    );
    Ok(())
}
