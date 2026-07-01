// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Avro schema → Iceberg schema conversion.
//!
//! Assigns monotonically increasing field IDs and adds metadata columns
//! (`_offset`, `_partition_id`, `_key`, `_ingest_time`).

use apache_avro::Schema as AvroSchema;
use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type as IcebergType};

use crate::error::{IcebergError, Result};

/// Metadata column field IDs start at this value.
/// User-schema fields get IDs starting from 1.
const META_FIELD_ID_BASE: i32 = 10_000;

/// Convert an Avro schema to an Iceberg schema with metadata columns.
///
/// Returns `(schema, next_field_id)` so callers can continue assigning
/// IDs for evolution operations.
pub fn avro_to_iceberg_schema(avro: &AvroSchema) -> Result<(IcebergSchema, i32)> {
    let record = match avro {
        AvroSchema::Record(r) => r,
        _ => {
            return Err(IcebergError::Schema(
                "root Avro schema must be a Record".into(),
            ));
        }
    };

    let mut next_id = 1i32;
    let mut fields = Vec::with_capacity(record.fields.len() + 4);

    for field in &record.fields {
        let (iceberg_type, new_next) = avro_type_to_iceberg(&field.schema, next_id + 1)?;
        let nullable = matches!(&field.schema, AvroSchema::Union(_));
        let iceberg_field = if nullable {
            NestedField::optional(next_id, &field.name, iceberg_type)
        } else {
            NestedField::required(next_id, &field.name, iceberg_type)
        };
        fields.push(iceberg_field.into());
        next_id = new_next;
    }

    // Metadata columns
    fields.push(
        NestedField::required(
            META_FIELD_ID_BASE,
            "_offset",
            IcebergType::Primitive(PrimitiveType::Long),
        )
        .into(),
    );
    fields.push(
        NestedField::required(
            META_FIELD_ID_BASE + 1,
            "_partition_id",
            IcebergType::Primitive(PrimitiveType::Int),
        )
        .into(),
    );
    fields.push(
        NestedField::required(
            META_FIELD_ID_BASE + 2,
            "_ingest_time",
            IcebergType::Primitive(PrimitiveType::Timestamptz),
        )
        .into(),
    );
    fields.push(
        NestedField::optional(
            META_FIELD_ID_BASE + 3,
            "_key",
            IcebergType::Primitive(PrimitiveType::Binary),
        )
        .into(),
    );

    let schema = IcebergSchema::builder()
        .with_fields(fields)
        .build()
        .map_err(|e| IcebergError::Schema(format!("failed to build iceberg schema: {e}")))?;

    Ok((schema, next_id))
}

/// Convert an Avro type to an Iceberg type.
///
/// `next_id` is the next available field ID for nested structs.
/// Returns `(type, updated_next_id)`.
fn avro_type_to_iceberg(avro: &AvroSchema, mut next_id: i32) -> Result<(IcebergType, i32)> {
    let ty = match avro {
        AvroSchema::Null => IcebergType::Primitive(PrimitiveType::Boolean), // placeholder, nullable
        AvroSchema::Boolean => IcebergType::Primitive(PrimitiveType::Boolean),
        AvroSchema::Int => IcebergType::Primitive(PrimitiveType::Int),
        AvroSchema::Long => IcebergType::Primitive(PrimitiveType::Long),
        AvroSchema::Float => IcebergType::Primitive(PrimitiveType::Float),
        AvroSchema::Double => IcebergType::Primitive(PrimitiveType::Double),
        AvroSchema::Bytes => IcebergType::Primitive(PrimitiveType::Binary),
        AvroSchema::String => IcebergType::Primitive(PrimitiveType::String),
        AvroSchema::Uuid => IcebergType::Primitive(PrimitiveType::Uuid),
        AvroSchema::Date => IcebergType::Primitive(PrimitiveType::Date),
        AvroSchema::TimeMillis => IcebergType::Primitive(PrimitiveType::Time),
        AvroSchema::TimeMicros => IcebergType::Primitive(PrimitiveType::Time),
        AvroSchema::TimestampMillis | AvroSchema::LocalTimestampMillis => {
            IcebergType::Primitive(PrimitiveType::Timestamptz)
        }
        AvroSchema::TimestampMicros | AvroSchema::LocalTimestampMicros => {
            IcebergType::Primitive(PrimitiveType::Timestamptz)
        }
        AvroSchema::TimestampNanos | AvroSchema::LocalTimestampNanos => {
            IcebergType::Primitive(PrimitiveType::Timestamptz)
        }
        AvroSchema::Decimal(d) => IcebergType::Primitive(PrimitiveType::Decimal {
            precision: d.precision as u32,
            scale: d.scale as u32,
        }),
        AvroSchema::Enum { .. } => IcebergType::Primitive(PrimitiveType::String),
        AvroSchema::Fixed(f) => IcebergType::Primitive(PrimitiveType::Fixed(f.size as u64)),

        AvroSchema::Union(union) => {
            // Collapse ["null", T] → T (nullable handled at field level)
            let non_null: Vec<_> = union
                .variants()
                .iter()
                .filter(|s| !matches!(s, AvroSchema::Null))
                .collect();
            if non_null.len() == 1 {
                return avro_type_to_iceberg(non_null[0], next_id);
            }
            // Complex unions not supported by Iceberg
            return Err(IcebergError::Schema(
                "complex unions (more than [null, T]) are not supported by Iceberg".into(),
            ));
        }

        AvroSchema::Record(record) => {
            let mut struct_fields = Vec::with_capacity(record.fields.len());
            for field in &record.fields {
                let field_id = next_id;
                next_id += 1;
                let (field_type, new_next) = avro_type_to_iceberg(&field.schema, next_id)?;
                next_id = new_next;
                let nullable = matches!(&field.schema, AvroSchema::Union(_));
                let nested = if nullable {
                    NestedField::optional(field_id, &field.name, field_type)
                } else {
                    NestedField::required(field_id, &field.name, field_type)
                };
                struct_fields.push(nested.into());
            }
            IcebergType::Struct(iceberg::spec::StructType::new(struct_fields))
        }

        AvroSchema::Array(arr) => {
            let elem_id = next_id;
            next_id += 1;
            let (elem_type, new_next) = avro_type_to_iceberg(&arr.items, next_id)?;
            next_id = new_next;
            IcebergType::List(iceberg::spec::ListType {
                element_field: NestedField::required(elem_id, "element", elem_type).into(),
            })
        }

        AvroSchema::Map(map) => {
            let key_id = next_id;
            next_id += 1;
            let val_id = next_id;
            next_id += 1;
            let (val_type, new_next) = avro_type_to_iceberg(&map.types, next_id)?;
            next_id = new_next;
            IcebergType::Map(iceberg::spec::MapType {
                key_field: NestedField::required(
                    key_id,
                    "key",
                    IcebergType::Primitive(PrimitiveType::String),
                )
                .into(),
                value_field: NestedField::optional(val_id, "value", val_type).into(),
            })
        }

        _ => IcebergType::Primitive(PrimitiveType::String), // fallback
    };

    Ok((ty, next_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_record() {
        let avro = AvroSchema::parse_str(
            r#"{
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "email", "type": ["null", "string"]}
            ]
        }"#,
        )
        .unwrap();

        let (schema, _next_id) = avro_to_iceberg_schema(&avro).unwrap();
        let fields = schema.as_struct();

        // 3 user fields + 4 metadata fields
        assert_eq!(fields.fields().len(), 7);

        // id: required long
        let id_field = &fields[0];
        assert_eq!(id_field.name, "id");
        assert!(id_field.required);

        // email: optional string
        let email_field = &fields[2];
        assert_eq!(email_field.name, "email");
        assert!(!email_field.required);

        // _offset: required long
        let offset_field = &fields[3];
        assert_eq!(offset_field.name, "_offset");
        assert!(offset_field.required);
    }

    #[test]
    fn test_nested_record() {
        let avro = AvroSchema::parse_str(
            r#"{
            "type": "record",
            "name": "Event",
            "fields": [
                {"name": "id", "type": "string"},
                {
                    "name": "payload",
                    "type": {
                        "type": "record",
                        "name": "Payload",
                        "fields": [
                            {"name": "data", "type": "bytes"}
                        ]
                    }
                }
            ]
        }"#,
        )
        .unwrap();

        let (schema, _) = avro_to_iceberg_schema(&avro).unwrap();
        assert_eq!(schema.as_struct().fields().len(), 6); // 2 user + 4 meta
    }

    #[test]
    fn test_complex_union_rejected() {
        let avro = AvroSchema::parse_str(
            r#"{
            "type": "record",
            "name": "Bad",
            "fields": [
                {"name": "value", "type": ["null", "string", "long"]}
            ]
        }"#,
        )
        .unwrap();

        assert!(avro_to_iceberg_schema(&avro).is_err());
    }

    #[test]
    fn test_array_and_map() {
        let avro = AvroSchema::parse_str(
            r#"{
            "type": "record",
            "name": "Container",
            "fields": [
                {"name": "tags", "type": {"type": "array", "items": "string"}},
                {"name": "attrs", "type": {"type": "map", "values": "long"}}
            ]
        }"#,
        )
        .unwrap();

        let (schema, _) = avro_to_iceberg_schema(&avro).unwrap();
        assert_eq!(schema.as_struct().fields().len(), 6); // 2 user + 4 meta
    }

    #[test]
    fn test_logical_types() {
        let avro = AvroSchema::parse_str(
            r#"{
            "type": "record",
            "name": "Temporal",
            "fields": [
                {"name": "d", "type": {"type": "int", "logicalType": "date"}},
                {"name": "ts", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                {"name": "uid", "type": {"type": "string", "logicalType": "uuid"}}
            ]
        }"#,
        )
        .unwrap();

        let (schema, _) = avro_to_iceberg_schema(&avro).unwrap();
        assert_eq!(schema.as_struct().fields().len(), 7); // 3 user + 4 meta
    }

    #[test]
    fn test_non_record_rejected() {
        assert!(avro_to_iceberg_schema(&AvroSchema::String).is_err());
    }
}
