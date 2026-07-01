// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Convert Avro-encoded record bytes to Arrow RecordBatches.
//!
//! Uses `flux_core::avro::BatchDeserializer` for fast decoding (5-9x faster
//! than `apache_avro::from_avro_datum`). Schema resolution is handled via
//! pre-computed field mappings from writer → reader layout.
//!
//! Adds metadata columns (`_offset`, `_partition_id`, `_ingest_time`, `_key`).

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use arrow::record_batch::RecordBatch as ArrowRecordBatch;
use chrono::{DateTime, Utc};

use flux_common::ids::SchemaId;
use flux_common::types::Record;
use flux_core::avro::Schema as CoreSchema;
use flux_core::avro::value::{BatchDeserializer, BumpValue};

use crate::error::{IcebergError, Result};

/// How to populate a single reader field from writer data.
#[derive(Debug, Clone)]
enum FieldSource {
    /// Read from writer field at this index, with optional type promotion.
    WriterField {
        index: usize,
        promote: Option<Promotion>,
    },
    /// Field not in writer — use this default value.
    Default(DefaultValue),
}

/// Supported Avro type promotions.
#[derive(Debug, Clone, Copy)]
enum Promotion {
    IntToLong,
    FloatToDouble,
}

/// Default value for fields missing from the writer schema.
#[derive(Debug, Clone)]
enum DefaultValue {
    Null,
    Bool(bool),
    Int(i32),
    Long(i64),
    Double(f64),
    String(String),
}

/// Pre-computed mapping from reader field positions to writer field positions.
#[derive(Debug, Clone)]
struct FieldMapping {
    /// One entry per reader field: how to populate it.
    sources: Vec<FieldSource>,
}

/// Per-writer-schema state: the parsed schema + field mapping to reader layout.
struct WriterState {
    schema: CoreSchema,
    mapping: FieldMapping,
}

/// Converts Flux records to Arrow batches with metadata columns.
///
/// Every record carries a `schema_id`, so we always know the exact writer
/// schema. We decode with the writer's own schema using `BatchDeserializer`,
/// then remap fields to the reader layout using pre-computed index mappings.
pub struct RecordConverter {
    /// Reader (latest) schema ID (used during writer registration).
    #[allow(dead_code)]
    reader_schema_id: SchemaId,
    /// Number of user fields in the reader schema.
    reader_field_count: usize,
    /// Arrow schema for the output (reader fields + metadata).
    arrow_schema: Arc<ArrowSchema>,
    /// Reader field names in order (for building mappings).
    reader_field_names: Vec<String>,
    /// Reader field Avro type names (for detecting promotions).
    reader_field_types: Vec<String>,
    /// Reader field defaults from the schema JSON.
    reader_field_defaults: Vec<Option<serde_json::Value>>,

    /// Per-writer-schema: decoder + field mapping to reader layout.
    writers: HashMap<SchemaId, WriterState>,
}

impl RecordConverter {
    /// The output Arrow schema (reader fields + metadata columns).
    pub fn arrow_schema(&self) -> &Arc<ArrowSchema> {
        &self.arrow_schema
    }

    /// Create a converter for a given reader schema.
    ///
    /// `reader_json` is the raw schema JSON (for extracting field info).
    /// `arrow_schema` is the pre-built Arrow schema for the output.
    pub fn new(reader_json: &serde_json::Value, reader_schema_id: SchemaId) -> Result<Self> {
        let reader_schema_str = reader_json.to_string();
        let core_schema: CoreSchema = reader_schema_str
            .parse()
            .map_err(|e| IcebergError::Schema(format!("parse reader schema: {e}")))?;

        let fields = get_fields(reader_json)?;
        let reader_field_count = fields.len();

        let reader_field_names: Vec<String> = fields
            .iter()
            .map(|f| f["name"].as_str().unwrap_or("").to_string())
            .collect();

        let reader_field_types: Vec<String> =
            fields.iter().map(|f| avro_type_name(&f["type"])).collect();

        let reader_field_defaults: Vec<Option<serde_json::Value>> =
            fields.iter().map(|f| f.get("default").cloned()).collect();

        let arrow_schema = Arc::new(build_arrow_schema_from_json(&fields)?);

        // Register reader schema as a writer (identity mapping)
        let identity_mapping = FieldMapping {
            sources: (0..reader_field_count)
                .map(|i| FieldSource::WriterField {
                    index: i,
                    promote: None,
                })
                .collect(),
        };
        let mut writers = HashMap::new();
        writers.insert(
            reader_schema_id,
            WriterState {
                schema: core_schema,
                mapping: identity_mapping,
            },
        );

        Ok(Self {
            reader_schema_id,
            reader_field_count,
            arrow_schema,
            reader_field_names,
            reader_field_types,
            reader_field_defaults,
            writers,
        })
    }

    /// Register a writer schema for resolution.
    ///
    /// Builds the field mapping from writer → reader layout, including
    /// type promotions and defaults for missing fields.
    pub fn register_writer_schema(
        &mut self,
        schema_id: SchemaId,
        writer_json: &serde_json::Value,
    ) -> Result<()> {
        let writer_schema_str = writer_json.to_string();
        let core_schema: CoreSchema = writer_schema_str
            .parse()
            .map_err(|e| IcebergError::Schema(format!("parse writer schema: {e}")))?;

        let writer_fields = get_fields(writer_json)?;

        // Build name→index lookup for writer, including aliases
        let mut writer_by_name: HashMap<&str, usize> = HashMap::new();
        for (i, f) in writer_fields.iter().enumerate() {
            if let Some(name) = f["name"].as_str() {
                writer_by_name.insert(name, i);
            }
            // Also index aliases
            if let Some(aliases) = f.get("aliases").and_then(|a| a.as_array()) {
                for alias in aliases {
                    if let Some(alias_str) = alias.as_str() {
                        writer_by_name.insert(alias_str, i);
                    }
                }
            }
        }

        // Also check for renames: if writer_json has flux.renames, the
        // old_name in writer might map to new_name in reader
        let renames = writer_json
            .get("flux.renames")
            .and_then(|v| v.as_object());

        let writer_field_types: Vec<String> = writer_fields
            .iter()
            .map(|f| avro_type_name(&f["type"]))
            .collect();

        let mapping = self.build_mapping(&writer_by_name, &writer_field_types, renames);

        self.writers.insert(
            schema_id,
            WriterState {
                schema: core_schema,
                mapping,
            },
        );
        Ok(())
    }

    /// Build a FieldMapping from writer → reader.
    fn build_mapping(
        &self,
        writer_by_name: &HashMap<&str, usize>,
        writer_field_types: &[String],
        renames: Option<&serde_json::Map<String, serde_json::Value>>,
    ) -> FieldMapping {
        let sources = (0..self.reader_field_count)
            .map(|reader_idx| {
                let reader_name = &self.reader_field_names[reader_idx];

                // Try direct name match first
                let writer_idx = writer_by_name
                    .get(reader_name.as_str())
                    .copied()
                    .or_else(|| {
                        // Check if this reader field was renamed from a writer field
                        renames.and_then(|r| {
                            r.iter().find_map(|(old_name, new_name)| {
                                if new_name.as_str() == Some(reader_name.as_str()) {
                                    writer_by_name.get(old_name.as_str()).copied()
                                } else {
                                    None
                                }
                            })
                        })
                    });

                match writer_idx {
                    Some(idx) => {
                        let promote = detect_promotion(
                            &writer_field_types[idx],
                            &self.reader_field_types[reader_idx],
                        );
                        FieldSource::WriterField {
                            index: idx,
                            promote,
                        }
                    }
                    None => {
                        // Field not in writer — use reader default
                        let default = self.reader_field_defaults[reader_idx]
                            .as_ref()
                            .map(json_to_default)
                            .unwrap_or(DefaultValue::Null);
                        FieldSource::Default(default)
                    }
                }
            })
            .collect();

        FieldMapping { sources }
    }

    /// Convert a batch of records to an Arrow RecordBatch.
    pub fn convert(
        &self,
        records: &[Record],
        schema_id: SchemaId,
        offsets: impl Iterator<Item = u64>,
        partition_id: i32,
        ingest_time: DateTime<Utc>,
    ) -> Result<ArrowRecordBatch> {
        let writer = self
            .writers
            .get(&schema_id)
            .ok_or_else(|| IcebergError::Schema(format!("unknown writer schema: {}", schema_id)))?;

        let batch_de = BatchDeserializer::new(&writer.schema);
        let mapping = &writer.mapping;

        let mut builders = create_builders(&self.arrow_schema)?;
        let user_field_count = self.reader_field_count;
        let ingest_ts_micros = ingest_time.timestamp_micros();

        for (record, offset) in records.iter().zip(offsets) {
            let value = batch_de
                .deserialize(record.value.as_ref())
                .map_err(|e| IcebergError::Conversion(format!("avro decode: {e}")))?;

            let fields = match &value {
                BumpValue::Record(f) => f,
                _ => return Err(IcebergError::Conversion("expected Avro Record".into())),
            };

            // Remap writer fields → reader columns using pre-computed mapping
            for (reader_idx, source) in mapping.sources.iter().enumerate() {
                match source {
                    FieldSource::WriterField { index, promote } => {
                        let v = &fields[*index];
                        append_bump_value(&mut builders[reader_idx], v, *promote)?;
                    }
                    FieldSource::Default(default) => {
                        append_default(&mut builders[reader_idx], default)?;
                    }
                }
            }

            // Metadata columns
            builders[user_field_count]
                .as_any_mut()
                .downcast_mut::<Int64Builder>()
                .unwrap()
                .append_value(offset as i64);

            builders[user_field_count + 1]
                .as_any_mut()
                .downcast_mut::<Int32Builder>()
                .unwrap()
                .append_value(partition_id);

            builders[user_field_count + 2]
                .as_any_mut()
                .downcast_mut::<TimestampMicrosecondBuilder>()
                .unwrap()
                .append_value(ingest_ts_micros);

            let key_builder = builders[user_field_count + 3]
                .as_any_mut()
                .downcast_mut::<BinaryBuilder>()
                .unwrap();
            match &record.key {
                Some(k) => key_builder.append_value(k.as_ref()),
                None => key_builder.append_null(),
            }
        }

        let arrays: Vec<ArrayRef> = builders.iter_mut().map(|b| b.finish()).collect();
        Ok(ArrowRecordBatch::try_new(
            self.arrow_schema.clone(),
            arrays,
        )?)
    }
}

// ============================================================================
// Schema parsing helpers
// ============================================================================

/// Extract the "fields" array from a record schema JSON.
fn get_fields(schema: &serde_json::Value) -> Result<Vec<serde_json::Value>> {
    schema
        .get("fields")
        .and_then(|f| f.as_array())
        .cloned()
        .ok_or_else(|| IcebergError::Schema("schema missing 'fields' array".into()))
}

/// Get the canonical Avro type name from a JSON type value.
fn avro_type_name(type_val: &serde_json::Value) -> String {
    match type_val {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Object(obj) => {
            if let Some(lt) = obj.get("logicalType").and_then(|v| v.as_str()) {
                lt.to_string()
            } else {
                obj.get("type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string()
            }
        }
        serde_json::Value::Array(arr) => {
            // Union: find the non-null type
            let non_null: Vec<_> = arr.iter().filter(|v| v.as_str() != Some("null")).collect();
            if non_null.len() == 1 {
                avro_type_name(non_null[0])
            } else {
                "union".to_string()
            }
        }
        _ => "unknown".to_string(),
    }
}

/// Detect type promotion between writer and reader types.
fn detect_promotion(writer_type: &str, reader_type: &str) -> Option<Promotion> {
    match (writer_type, reader_type) {
        ("int", "long") => Some(Promotion::IntToLong),
        ("float", "double") => Some(Promotion::FloatToDouble),
        _ => None,
    }
}

/// Convert a JSON default value to a DefaultValue.
fn json_to_default(val: &serde_json::Value) -> DefaultValue {
    match val {
        serde_json::Value::Null => DefaultValue::Null,
        serde_json::Value::Bool(b) => DefaultValue::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                    DefaultValue::Int(i as i32)
                } else {
                    DefaultValue::Long(i)
                }
            } else if let Some(f) = n.as_f64() {
                DefaultValue::Double(f)
            } else {
                DefaultValue::Null
            }
        }
        serde_json::Value::String(s) => DefaultValue::String(s.clone()),
        _ => DefaultValue::Null,
    }
}

// ============================================================================
// Arrow schema building from JSON
// ============================================================================

fn build_arrow_schema_from_json(fields: &[serde_json::Value]) -> Result<ArrowSchema> {
    let mut arrow_fields: Vec<Field> = fields
        .iter()
        .map(|f| {
            let name = f["name"].as_str().unwrap_or("unknown");
            let type_val = &f["type"];
            let dt = json_type_to_arrow(type_val);
            let nullable = is_nullable(type_val);
            Field::new(name, dt, nullable)
        })
        .collect();

    // Metadata columns
    arrow_fields.push(Field::new("_offset", DataType::Int64, false));
    arrow_fields.push(Field::new("_partition_id", DataType::Int32, false));
    arrow_fields.push(Field::new(
        "_ingest_time",
        DataType::Timestamp(TimeUnit::Microsecond, None),
        false,
    ));
    arrow_fields.push(Field::new("_key", DataType::Binary, true));

    Ok(ArrowSchema::new(arrow_fields))
}

fn is_nullable(type_val: &serde_json::Value) -> bool {
    if let serde_json::Value::Array(arr) = type_val {
        arr.iter().any(|v| v.as_str() == Some("null"))
    } else {
        false
    }
}

fn json_type_to_arrow(type_val: &serde_json::Value) -> DataType {
    match type_val {
        serde_json::Value::String(s) => primitive_type_to_arrow(s),
        serde_json::Value::Object(obj) => {
            if let Some(lt) = obj.get("logicalType").and_then(|v| v.as_str()) {
                match lt {
                    "date" => DataType::Date32,
                    "time-millis" => DataType::Time32(TimeUnit::Millisecond),
                    "time-micros" => DataType::Time64(TimeUnit::Microsecond),
                    "timestamp-millis" | "local-timestamp-millis" => {
                        DataType::Timestamp(TimeUnit::Millisecond, None)
                    }
                    "timestamp-micros" | "local-timestamp-micros" => {
                        DataType::Timestamp(TimeUnit::Microsecond, None)
                    }
                    "timestamp-nanos" | "local-timestamp-nanos" => {
                        DataType::Timestamp(TimeUnit::Nanosecond, None)
                    }
                    "uuid" => DataType::Utf8,
                    "decimal" => {
                        let p = obj.get("precision").and_then(|v| v.as_u64()).unwrap_or(38) as u8;
                        let s = obj.get("scale").and_then(|v| v.as_i64()).unwrap_or(0) as i8;
                        DataType::Decimal128(p, s)
                    }
                    _ => DataType::Utf8,
                }
            } else {
                match obj.get("type").and_then(|v| v.as_str()) {
                    Some("record") => DataType::Utf8, // nested records → fallback
                    Some("array") => {
                        let default_string = serde_json::Value::String("string".into());
                        let items = obj.get("items").unwrap_or(&default_string);
                        let inner = json_type_to_arrow(items);
                        DataType::List(Arc::new(Field::new("item", inner, true)))
                    }
                    Some("map") => {
                        let default_string = serde_json::Value::String("string".into());
                        let values = obj.get("values").unwrap_or(&default_string);
                        let val = json_type_to_arrow(values);
                        DataType::Map(
                            Arc::new(Field::new(
                                "entries",
                                DataType::Struct(
                                    vec![
                                        Field::new("key", DataType::Utf8, false),
                                        Field::new("value", val, true),
                                    ]
                                    .into(),
                                ),
                                false,
                            )),
                            false,
                        )
                    }
                    Some("enum") => DataType::Utf8,
                    Some("fixed") => {
                        let size = obj.get("size").and_then(|v| v.as_i64()).unwrap_or(16) as i32;
                        DataType::FixedSizeBinary(size)
                    }
                    Some(other) => primitive_type_to_arrow(other),
                    None => DataType::Utf8,
                }
            }
        }
        serde_json::Value::Array(arr) => {
            // Union: find non-null type
            let non_null: Vec<_> = arr.iter().filter(|v| v.as_str() != Some("null")).collect();
            if non_null.len() == 1 {
                json_type_to_arrow(non_null[0])
            } else {
                DataType::Utf8
            }
        }
        _ => DataType::Utf8,
    }
}

fn primitive_type_to_arrow(s: &str) -> DataType {
    match s {
        "null" => DataType::Null,
        "boolean" => DataType::Boolean,
        "int" => DataType::Int32,
        "long" => DataType::Int64,
        "float" => DataType::Float32,
        "double" => DataType::Float64,
        "bytes" => DataType::Binary,
        "string" => DataType::Utf8,
        _ => DataType::Utf8,
    }
}

// ============================================================================
// Arrow builder helpers
// ============================================================================

fn create_builders(schema: &ArrowSchema) -> Result<Vec<Box<dyn ArrayBuilder>>> {
    schema
        .fields()
        .iter()
        .map(|f| create_builder(f.data_type()))
        .collect()
}

fn create_builder(dt: &DataType) -> Result<Box<dyn ArrayBuilder>> {
    Ok(match dt {
        DataType::Null => Box::new(NullBuilder::new()),
        DataType::Boolean => Box::new(BooleanBuilder::new()),
        DataType::Int32 => Box::new(Int32Builder::new()),
        DataType::Int64 => Box::new(Int64Builder::new()),
        DataType::Float32 => Box::new(Float32Builder::new()),
        DataType::Float64 => Box::new(Float64Builder::new()),
        DataType::Binary => Box::new(BinaryBuilder::new()),
        DataType::Utf8 => Box::new(StringBuilder::new()),
        DataType::Date32 => Box::new(Date32Builder::new()),
        DataType::Time32(TimeUnit::Millisecond) => Box::new(Time32MillisecondBuilder::new()),
        DataType::Time64(TimeUnit::Microsecond) => Box::new(Time64MicrosecondBuilder::new()),
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            Box::new(TimestampMillisecondBuilder::new())
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            Box::new(TimestampMicrosecondBuilder::new())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => Box::new(TimestampNanosecondBuilder::new()),
        DataType::FixedSizeBinary(size) => Box::new(FixedSizeBinaryBuilder::new(*size)),
        DataType::Decimal128(p, s) => {
            Box::new(Decimal128Builder::new().with_precision_and_scale(*p, *s)?)
        }
        DataType::List(field) => Box::new(ListBuilder::new(create_builder(field.data_type())?)),
        _ => Box::new(StringBuilder::new()),
    })
}

/// Append a BumpValue to an Arrow builder, with optional type promotion.
fn append_bump_value(
    builder: &mut Box<dyn ArrayBuilder>,
    value: &BumpValue<'_>,
    promote: Option<Promotion>,
) -> Result<()> {
    match value {
        BumpValue::Null => append_null(builder),
        BumpValue::Boolean(v) => try_append!(builder, BooleanBuilder, *v),
        BumpValue::Int(v) => match promote {
            Some(Promotion::IntToLong) => {
                try_append!(builder, Int64Builder, *v as i64);
            }
            _ => {
                try_append!(builder, Int32Builder, *v);
                try_append!(builder, Date32Builder, *v);
                try_append!(builder, Time32MillisecondBuilder, *v);
            }
        },
        BumpValue::Long(v) => {
            try_append!(builder, Int64Builder, *v);
            try_append!(builder, TimestampMillisecondBuilder, *v);
            try_append!(builder, TimestampMicrosecondBuilder, *v);
            try_append!(builder, TimestampNanosecondBuilder, *v);
            try_append!(builder, Time64MicrosecondBuilder, *v);
        }
        BumpValue::Float(v) => match promote {
            Some(Promotion::FloatToDouble) => {
                try_append!(builder, Float64Builder, *v as f64);
            }
            _ => try_append!(builder, Float32Builder, *v),
        },
        BumpValue::Double(v) => try_append!(builder, Float64Builder, *v),
        BumpValue::Bytes(v) => try_append!(builder, BinaryBuilder, *v),
        BumpValue::String(v) => try_append!(builder, StringBuilder, *v),
        BumpValue::Enum { symbol, .. } => try_append!(builder, StringBuilder, *symbol),
        BumpValue::Union { value: inner, .. } => {
            return append_bump_value(builder, inner, promote);
        }
        BumpValue::Fixed { data, .. } => {
            if let Some(b) = builder
                .as_any_mut()
                .downcast_mut::<FixedSizeBinaryBuilder>()
            {
                b.append_value(data)?;
            }
        }
        BumpValue::Date(v) => try_append!(builder, Date32Builder, *v),
        BumpValue::TimestampMillis(v) => try_append!(builder, TimestampMillisecondBuilder, *v),
        BumpValue::TimestampMicros(v) => try_append!(builder, TimestampMicrosecondBuilder, *v),
        BumpValue::TimeMillis(v) => try_append!(builder, Time32MillisecondBuilder, *v),
        BumpValue::TimeMicros(v) => try_append!(builder, Time64MicrosecondBuilder, *v),
        BumpValue::Uuid(v) => try_append!(builder, StringBuilder, *v),
        BumpValue::Decimal(v) => {
            if let Some(b) = builder.as_any_mut().downcast_mut::<Decimal128Builder>() {
                // Convert rust_decimal to i128 representation
                b.append_value(decimal_to_i128(*v));
            }
        }
        _ => try_append!(builder, StringBuilder, format!("{:?}", value)),
    }
    Ok(())
}

/// Convert rust_decimal::Decimal to i128 for Arrow Decimal128.
fn decimal_to_i128(d: rust_decimal::Decimal) -> i128 {
    d.mantissa()
}

macro_rules! try_append {
    ($builder:expr, $type:ty, $value:expr) => {
        if let Some(b) = $builder.as_any_mut().downcast_mut::<$type>() {
            b.append_value($value);
        }
    };
}
use try_append;

/// Append a default value to an Arrow builder.
fn append_default(builder: &mut Box<dyn ArrayBuilder>, default: &DefaultValue) -> Result<()> {
    match default {
        DefaultValue::Null => append_null(builder),
        DefaultValue::Bool(v) => try_append!(builder, BooleanBuilder, *v),
        DefaultValue::Int(v) => {
            try_append!(builder, Int32Builder, *v);
            try_append!(builder, Int64Builder, *v as i64);
        }
        DefaultValue::Long(v) => {
            try_append!(builder, Int64Builder, *v);
            try_append!(builder, TimestampMillisecondBuilder, *v);
            try_append!(builder, TimestampMicrosecondBuilder, *v);
        }
        DefaultValue::Double(v) => try_append!(builder, Float64Builder, *v),
        DefaultValue::String(v) => try_append!(builder, StringBuilder, v.as_str()),
    }
    Ok(())
}

fn append_null(builder: &mut Box<dyn ArrayBuilder>) {
    macro_rules! try_null {
        ($($type:ty),*) => {
            $(if let Some(b) = builder.as_any_mut().downcast_mut::<$type>() { b.append_null(); return; })*
        };
    }
    try_null!(
        StringBuilder,
        Int32Builder,
        Int64Builder,
        Float32Builder,
        Float64Builder,
        BooleanBuilder,
        BinaryBuilder,
        Date32Builder,
        TimestampMillisecondBuilder,
        TimestampMicrosecondBuilder,
        TimestampNanosecondBuilder
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::{Schema as AvroSchema, to_avro_datum, types::Value as AvroValue};
    use bytes::Bytes;

    fn simple_schema_json() -> serde_json::Value {
        serde_json::json!({
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"}
            ]
        })
    }

    #[test]
    fn test_convert_simple_records() {
        let schema_json = simple_schema_json();
        let converter = RecordConverter::new(&schema_json, SchemaId(100)).unwrap();

        // Encode with apache_avro for test data
        let avro_schema = AvroSchema::parse_str(&schema_json.to_string()).unwrap();
        let record = AvroValue::Record(vec![
            ("id".into(), AvroValue::Long(42)),
            ("name".into(), AvroValue::String("Alice".into())),
        ]);
        let bytes = to_avro_datum(&avro_schema, record).unwrap();

        let records = vec![
            Record::new(Bytes::from(bytes.clone())),
            Record::with_key("key1", Bytes::from(bytes)),
        ];

        let batch = converter
            .convert(&records, SchemaId(100), 0..2, 1, Utc::now())
            .unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 6); // 2 user + 4 metadata

        // Check user fields
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 42);
        assert_eq!(ids.value(1), 42);

        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "Alice");

        // Check _offset
        let offsets = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(offsets.value(0), 0);
        assert_eq!(offsets.value(1), 1);
    }

    #[test]
    fn test_convert_with_schema_resolution() {
        let v1_json = serde_json::json!({
            "type": "record", "name": "User",
            "fields": [{"name": "id", "type": "long"}]
        });
        let v2_json = serde_json::json!({
            "type": "record", "name": "User",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string", "default": "unknown"}
            ]
        });

        let mut converter = RecordConverter::new(&v2_json, SchemaId(200)).unwrap();
        converter
            .register_writer_schema(SchemaId(100), &v1_json)
            .unwrap();

        // Encode record with v1 schema
        let v1_avro = AvroSchema::parse_str(&v1_json.to_string()).unwrap();
        let v1_record = AvroValue::Record(vec![("id".into(), AvroValue::Long(1))]);
        let v1_bytes = to_avro_datum(&v1_avro, v1_record).unwrap();

        let records = vec![Record::new(Bytes::from(v1_bytes))];
        let batch = converter
            .convert(&records, SchemaId(100), 0..1, 0, Utc::now())
            .unwrap();

        assert_eq!(batch.num_rows(), 1);
        // name should be filled with default "unknown"
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "unknown");
    }

    #[test]
    fn test_convert_with_type_promotion() {
        let v1_json = serde_json::json!({
            "type": "record", "name": "Metrics",
            "fields": [{"name": "count", "type": "int"}]
        });
        let v2_json = serde_json::json!({
            "type": "record", "name": "Metrics",
            "fields": [{"name": "count", "type": "long"}]
        });

        let mut converter = RecordConverter::new(&v2_json, SchemaId(200)).unwrap();
        converter
            .register_writer_schema(SchemaId(100), &v1_json)
            .unwrap();

        // Encode record with v1 (int)
        let v1_avro = AvroSchema::parse_str(&v1_json.to_string()).unwrap();
        let v1_record = AvroValue::Record(vec![("count".into(), AvroValue::Int(42))]);
        let v1_bytes = to_avro_datum(&v1_avro, v1_record).unwrap();

        let records = vec![Record::new(Bytes::from(v1_bytes))];
        let batch = converter
            .convert(&records, SchemaId(100), 0..1, 0, Utc::now())
            .unwrap();

        // count should be promoted to i64
        let counts = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(counts.value(0), 42);
    }

    #[test]
    fn test_identity_mapping_same_schema() {
        let schema_json = simple_schema_json();
        let converter = RecordConverter::new(&schema_json, SchemaId(100)).unwrap();

        // Verify identity mapping for the reader schema
        let writer = converter.writers.get(&SchemaId(100)).unwrap();
        assert_eq!(writer.mapping.sources.len(), 2);
        assert!(matches!(
            writer.mapping.sources[0],
            FieldSource::WriterField {
                index: 0,
                promote: None
            }
        ));
        assert!(matches!(
            writer.mapping.sources[1],
            FieldSource::WriterField {
                index: 1,
                promote: None
            }
        ));
    }
}
