// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

use std::sync::Arc;

use apache_avro::types::Value as AvroValue;
use apache_avro::{Schema as AvroSchema, from_avro_datum};
use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use arrow::record_batch::RecordBatch;

use crate::buffer::error::{FluxError, Result};

pub fn avro_schema_to_arrow(avro_schema: &AvroSchema) -> Result<ArrowSchema> {
    match avro_schema {
        AvroSchema::Record(record) => {
            let fields: Result<Vec<Field>> = record
                .fields
                .iter()
                .map(|f| {
                    let data_type = avro_type_to_arrow(&f.schema)?;
                    let nullable = matches!(&f.schema, AvroSchema::Union(_));
                    Ok(Field::new(&f.name, data_type, nullable))
                })
                .collect();
            Ok(ArrowSchema::new(fields?))
        }
        _ => Err(FluxError::InvalidSchema(
            "Root Avro schema must be a Record".to_string(),
        )),
    }
}

fn avro_type_to_arrow(avro_schema: &AvroSchema) -> Result<DataType> {
    match avro_schema {
        AvroSchema::Null => Ok(DataType::Null),
        AvroSchema::Boolean => Ok(DataType::Boolean),
        AvroSchema::Int => Ok(DataType::Int32),
        AvroSchema::Long => Ok(DataType::Int64),
        AvroSchema::Float => Ok(DataType::Float32),
        AvroSchema::Double => Ok(DataType::Float64),
        AvroSchema::Bytes => Ok(DataType::Binary),
        AvroSchema::String => Ok(DataType::Utf8),
        AvroSchema::Array(array_schema) => {
            let inner_type = avro_type_to_arrow(&array_schema.items)?;
            Ok(DataType::List(Arc::new(Field::new(
                "item", inner_type, true,
            ))))
        }
        AvroSchema::Map(map_schema) => {
            let value_type = avro_type_to_arrow(&map_schema.types)?;
            Ok(DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", value_type, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ))
        }
        AvroSchema::Union(union) => {
            let non_null: Vec<_> = union
                .variants()
                .iter()
                .filter(|s| !matches!(s, AvroSchema::Null))
                .collect();
            if non_null.len() == 1 {
                avro_type_to_arrow(non_null[0])
            } else {
                Ok(DataType::Utf8)
            }
        }
        AvroSchema::Record(record) => {
            let fields: Result<Vec<Field>> = record
                .fields
                .iter()
                .map(|f| {
                    let data_type = avro_type_to_arrow(&f.schema)?;
                    let nullable = matches!(&f.schema, AvroSchema::Union(_));
                    Ok(Field::new(&f.name, data_type, nullable))
                })
                .collect();
            Ok(DataType::Struct(fields?.into()))
        }
        AvroSchema::Enum { .. } => Ok(DataType::Utf8),
        AvroSchema::Fixed(fixed) => Ok(DataType::FixedSizeBinary(fixed.size as i32)),
        AvroSchema::Decimal(decimal) => Ok(DataType::Decimal128(
            decimal.precision as u8,
            decimal.scale as i8,
        )),
        AvroSchema::Uuid => Ok(DataType::Utf8),
        AvroSchema::Date => Ok(DataType::Date32),
        AvroSchema::TimeMillis => Ok(DataType::Time32(TimeUnit::Millisecond)),
        AvroSchema::TimeMicros => Ok(DataType::Time64(TimeUnit::Microsecond)),
        AvroSchema::TimestampMillis | AvroSchema::LocalTimestampMillis => {
            Ok(DataType::Timestamp(TimeUnit::Millisecond, None))
        }
        AvroSchema::TimestampMicros | AvroSchema::LocalTimestampMicros => {
            Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
        }
        AvroSchema::TimestampNanos | AvroSchema::LocalTimestampNanos => {
            Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
        }
        AvroSchema::Duration => Ok(DataType::Interval(
            arrow::datatypes::IntervalUnit::MonthDayNano,
        )),
        AvroSchema::BigDecimal => Ok(DataType::Utf8),
        AvroSchema::Ref { name } => Err(FluxError::InvalidSchema(format!(
            "Schema reference '{}' not supported",
            name.fullname(None)
        ))),
    }
}

pub struct RecordBatchBuilder {
    arrow_schema: Arc<ArrowSchema>,
    avro_schema: AvroSchema,
    builders: Vec<Box<dyn ArrayBuilder>>,
    row_count: usize,
}

impl RecordBatchBuilder {
    pub fn new(avro_schema: AvroSchema) -> Result<Self> {
        let arrow_schema = Arc::new(avro_schema_to_arrow(&avro_schema)?);
        let builders = create_builders(&arrow_schema)?;
        Ok(Self {
            arrow_schema,
            avro_schema,
            builders,
            row_count: 0,
        })
    }

    pub fn append_avro_bytes(&mut self, data: &[u8]) -> Result<()> {
        let mut reader = data;
        let value = from_avro_datum(&self.avro_schema, &mut reader, None)?;
        self.append_value(&value)
    }

    pub fn append_value(&mut self, value: &AvroValue) -> Result<()> {
        if let AvroValue::Record(fields) = value {
            for (i, (_, field_value)) in fields.iter().enumerate() {
                append_value_to_builder(&mut self.builders[i], field_value)?;
            }
            self.row_count += 1;
            Ok(())
        } else {
            Err(FluxError::Conversion(
                "Expected Avro Record value".to_string(),
            ))
        }
    }

    pub fn row_count(&self) -> usize {
        self.row_count
    }

    pub fn finish(mut self) -> Result<RecordBatch> {
        let arrays: Vec<ArrayRef> = self.builders.iter_mut().map(|b| b.finish()).collect();
        Ok(RecordBatch::try_new(self.arrow_schema, arrays)?)
    }
}

fn create_builders(schema: &ArrowSchema) -> Result<Vec<Box<dyn ArrayBuilder>>> {
    schema
        .fields()
        .iter()
        .map(|f| create_builder(f.data_type()))
        .collect()
}

fn create_builder(data_type: &DataType) -> Result<Box<dyn ArrayBuilder>> {
    Ok(match data_type {
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
        DataType::Time64(TimeUnit::Nanosecond) => Box::new(Time64NanosecondBuilder::new()),
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            Box::new(TimestampMillisecondBuilder::new())
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            Box::new(TimestampMicrosecondBuilder::new())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => Box::new(TimestampNanosecondBuilder::new()),
        DataType::FixedSizeBinary(size) => Box::new(FixedSizeBinaryBuilder::new(*size)),
        DataType::List(field) => Box::new(ListBuilder::new(create_builder(field.data_type())?)),
        _ => Box::new(StringBuilder::new()),
    })
}

fn append_value_to_builder(builder: &mut Box<dyn ArrayBuilder>, value: &AvroValue) -> Result<()> {
    match value {
        AvroValue::Null => append_null(builder),
        AvroValue::Boolean(v) => try_append!(builder, BooleanBuilder, *v),
        AvroValue::Int(v) => {
            try_append!(builder, Int32Builder, *v);
            try_append!(builder, Date32Builder, *v);
            try_append!(builder, Time32MillisecondBuilder, *v);
        }
        AvroValue::Long(v) => {
            try_append!(builder, Int64Builder, *v);
            try_append!(builder, TimestampMillisecondBuilder, *v);
            try_append!(builder, TimestampMicrosecondBuilder, *v);
            try_append!(builder, TimestampNanosecondBuilder, *v);
            try_append!(builder, Time64MicrosecondBuilder, *v);
            try_append!(builder, Time64NanosecondBuilder, *v);
        }
        AvroValue::Float(v) => try_append!(builder, Float32Builder, *v),
        AvroValue::Double(v) => try_append!(builder, Float64Builder, *v),
        AvroValue::Bytes(v) => try_append!(builder, BinaryBuilder, v),
        AvroValue::String(v) | AvroValue::Enum(_, v) => try_append!(builder, StringBuilder, v),
        AvroValue::Union(_, inner) => return append_value_to_builder(builder, inner),
        AvroValue::Fixed(_, v) => {
            if let Some(b) = builder
                .as_any_mut()
                .downcast_mut::<FixedSizeBinaryBuilder>()
            {
                b.append_value(v)?;
            }
        }
        AvroValue::Date(v) => try_append!(builder, Date32Builder, *v),
        AvroValue::TimestampMillis(v) | AvroValue::LocalTimestampMillis(v) => {
            try_append!(builder, TimestampMillisecondBuilder, *v)
        }
        AvroValue::TimestampMicros(v) | AvroValue::LocalTimestampMicros(v) => {
            try_append!(builder, TimestampMicrosecondBuilder, *v)
        }
        AvroValue::TimestampNanos(v) | AvroValue::LocalTimestampNanos(v) => {
            try_append!(builder, TimestampNanosecondBuilder, *v)
        }
        AvroValue::Uuid(v) => try_append!(builder, StringBuilder, v.to_string()),
        _ => try_append!(builder, StringBuilder, format!("{:?}", value)),
    }
    Ok(())
}

macro_rules! try_append {
    ($builder:expr, $type:ty, $value:expr) => {
        if let Some(b) = $builder.as_any_mut().downcast_mut::<$type>() {
            b.append_value($value);
        }
    };
}
use try_append;

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
        BinaryBuilder
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::to_avro_datum;

    fn sample_schema() -> AvroSchema {
        AvroSchema::parse_str(
            r#"{
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "email", "type": ["null", "string"]},
                {"name": "age", "type": "int"},
                {"name": "active", "type": "boolean"},
                {"name": "score", "type": "double"}
            ]
        }"#,
        )
        .unwrap()
    }

    #[test]
    fn test_schema_conversion() {
        let schema = sample_schema();
        let arrow_schema = avro_schema_to_arrow(&schema).unwrap();

        assert_eq!(arrow_schema.fields().len(), 6);
        assert_eq!(arrow_schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(arrow_schema.field(1).data_type(), &DataType::Utf8);
        assert!(arrow_schema.field(2).is_nullable());
        assert_eq!(arrow_schema.field(3).data_type(), &DataType::Int32);
        assert_eq!(arrow_schema.field(4).data_type(), &DataType::Boolean);
        assert_eq!(arrow_schema.field(5).data_type(), &DataType::Float64);
    }

    #[test]
    fn test_nested_schema() {
        let schema = AvroSchema::parse_str(r#"{
            "type": "record",
            "name": "Outer",
            "fields": [{
                "name": "inner",
                "type": {"type": "record", "name": "Inner", "fields": [{"name": "value", "type": "int"}]}
            }]
        }"#).unwrap();

        let arrow_schema = avro_schema_to_arrow(&schema).unwrap();
        match arrow_schema.field(0).data_type() {
            DataType::Struct(fields) => assert_eq!(fields[0].data_type(), &DataType::Int32),
            _ => panic!("Expected Struct type"),
        }
    }

    #[test]
    fn test_non_record_error() {
        assert!(avro_schema_to_arrow(&AvroSchema::String).is_err());
    }

    #[test]
    fn test_record_batch_builder() {
        let schema = sample_schema();
        let mut builder = RecordBatchBuilder::new(schema).unwrap();

        let record = AvroValue::Record(vec![
            ("id".to_string(), AvroValue::Long(1)),
            ("name".to_string(), AvroValue::String("Alice".to_string())),
            (
                "email".to_string(),
                AvroValue::Union(
                    1,
                    Box::new(AvroValue::String("alice@example.com".to_string())),
                ),
            ),
            ("age".to_string(), AvroValue::Int(30)),
            ("active".to_string(), AvroValue::Boolean(true)),
            ("score".to_string(), AvroValue::Double(95.5)),
        ]);

        builder.append_value(&record).unwrap();
        assert_eq!(builder.row_count(), 1);

        let batch = builder.finish().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 6);
    }

    #[test]
    fn test_avro_bytes_parsing() {
        let schema = AvroSchema::parse_str(
            r#"{
            "type": "record",
            "name": "Simple",
            "fields": [{"name": "id", "type": "long"}, {"name": "name", "type": "string"}]
        }"#,
        )
        .unwrap();

        let mut builder = RecordBatchBuilder::new(schema.clone()).unwrap();
        let record = AvroValue::Record(vec![
            ("id".to_string(), AvroValue::Long(42)),
            ("name".to_string(), AvroValue::String("Test".to_string())),
        ]);
        let bytes = to_avro_datum(&schema, record).unwrap();

        builder.append_avro_bytes(&bytes).unwrap();
        assert_eq!(builder.finish().unwrap().num_rows(), 1);
    }
}
