//! Benchmarks for RecordConverter::convert() vs apache_avro baseline.
//!
//! Run with: cargo bench --bench record_converter -p flux-iceberg

use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;

use apache_avro::{
    Schema as AvroSchema, from_avro_datum, to_avro_datum, types::Value as AvroValue,
};
use arrow::array::*;
use arrow::datatypes::{DataType, Schema as ArrowSchema, TimeUnit};
use arrow::record_batch::RecordBatch as ArrowRecordBatch;
use bytes::Bytes;
use chrono::Utc;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rand::Rng;

use flux_common::ids::SchemaId;
use flux_common::types::Record;
use flux_iceberg::RecordConverter;

// ============================================================================
// Data generation helpers (not measured)
// ============================================================================

/// Build a record schema JSON with `field_count` fields.
///
/// First min(field_count, 5) fields are typed (id:long, timestamp:timestamp-millis,
/// user_id:string, action:string, amount:double). Remaining fields are strings
/// named `field_N`.
fn make_schema_json(field_count: usize) -> serde_json::Value {
    let typed_fields: Vec<serde_json::Value> = vec![
        serde_json::json!({"name": "id", "type": "long"}),
        serde_json::json!({"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}}),
        serde_json::json!({"name": "user_id", "type": "string"}),
        serde_json::json!({"name": "action", "type": "string"}),
        serde_json::json!({"name": "amount", "type": "double"}),
    ];

    let mut fields: Vec<serde_json::Value> = typed_fields.into_iter().take(field_count).collect();

    for i in fields.len()..field_count {
        fields.push(serde_json::json!({"name": format!("field_{i}"), "type": "string"}));
    }

    serde_json::json!({
        "type": "record",
        "name": "BenchRecord",
        "fields": fields
    })
}

/// Generate a random string of given length.
fn random_string(rng: &mut impl Rng, len: usize) -> String {
    (0..len)
        .map(|_| rng.gen_range(b'a'..=b'z') as char)
        .collect()
}

/// Build an AvroValue::Record matching `make_schema_json(field_count)`.
fn make_avro_record(rng: &mut impl Rng, field_count: usize, string_len: usize) -> AvroValue {
    let mut fields = Vec::with_capacity(field_count);

    // Typed fields (up to 5)
    let typed: Vec<(&str, AvroValue)> = vec![
        ("id", AvroValue::Long(rng.gen_range(1..1_000_000))),
        (
            "timestamp",
            AvroValue::TimestampMillis(chrono::Utc::now().timestamp_millis()),
        ),
        ("user_id", AvroValue::String(random_string(rng, string_len))),
        ("action", AvroValue::String(random_string(rng, string_len))),
        ("amount", AvroValue::Double(rng.gen_range(0.0..10_000.0))),
    ];

    for (name, val) in typed.into_iter().take(field_count) {
        fields.push((name.to_string(), val));
    }

    for i in fields.len()..field_count {
        fields.push((
            format!("field_{i}"),
            AvroValue::String(random_string(rng, string_len)),
        ));
    }

    AvroValue::Record(fields)
}

/// Pre-encode test records. Returns (schema_json, avro_schema, records).
fn make_test_data(
    field_count: usize,
    record_count: usize,
    string_len: usize,
) -> (serde_json::Value, AvroSchema, Vec<Record>) {
    let schema_json = make_schema_json(field_count);
    let avro_schema = AvroSchema::parse_str(&schema_json.to_string()).unwrap();
    let mut rng = rand::thread_rng();

    let records: Vec<Record> = (0..record_count)
        .map(|_| {
            let val = make_avro_record(&mut rng, field_count, string_len);
            let bytes = to_avro_datum(&avro_schema, val).unwrap();
            Record::new(Bytes::from(bytes))
        })
        .collect();

    (schema_json, avro_schema, records)
}

// ============================================================================
// Baseline: old approach using apache_avro::from_avro_datum
// ============================================================================

/// Old-style conversion: per-record `from_avro_datum` → walk Value → Arrow append.
/// This is what RecordConverter replaced.
fn baseline_convert(
    records: &[Record],
    avro_schema: &AvroSchema,
    arrow_schema: &Arc<ArrowSchema>,
    field_count: usize,
) -> ArrowRecordBatch {
    let mut builders: Vec<Box<dyn ArrayBuilder>> = arrow_schema
        .fields()
        .iter()
        .map(|f| make_builder(f.data_type()))
        .collect();

    let ingest_ts = Utc::now().timestamp_micros();

    for (i, record) in records.iter().enumerate() {
        let mut cursor = Cursor::new(record.value.as_ref());
        let value = from_avro_datum(avro_schema, &mut cursor, None).unwrap();

        if let AvroValue::Record(fields) = value {
            for (idx, (_name, val)) in fields.into_iter().enumerate().take(field_count) {
                baseline_append(&mut builders[idx], val);
            }
        }

        // Metadata columns
        builders[field_count]
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .unwrap()
            .append_value(i as i64);
        builders[field_count + 1]
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .unwrap()
            .append_value(0);
        builders[field_count + 2]
            .as_any_mut()
            .downcast_mut::<TimestampMicrosecondBuilder>()
            .unwrap()
            .append_value(ingest_ts);
        builders[field_count + 3]
            .as_any_mut()
            .downcast_mut::<BinaryBuilder>()
            .unwrap()
            .append_null();
    }

    let arrays: Vec<ArrayRef> = builders.iter_mut().map(|b| b.finish()).collect();
    ArrowRecordBatch::try_new(arrow_schema.clone(), arrays).unwrap()
}

fn baseline_append(builder: &mut Box<dyn ArrayBuilder>, val: AvroValue) {
    match val {
        AvroValue::Long(v) => {
            if let Some(b) = builder.as_any_mut().downcast_mut::<Int64Builder>() {
                b.append_value(v);
            } else if let Some(b) = builder
                .as_any_mut()
                .downcast_mut::<TimestampMillisecondBuilder>()
            {
                b.append_value(v);
            }
        }
        AvroValue::TimestampMillis(v) => {
            if let Some(b) = builder
                .as_any_mut()
                .downcast_mut::<TimestampMillisecondBuilder>()
            {
                b.append_value(v);
            } else if let Some(b) = builder.as_any_mut().downcast_mut::<Int64Builder>() {
                b.append_value(v);
            }
        }
        AvroValue::String(v) => {
            if let Some(b) = builder.as_any_mut().downcast_mut::<StringBuilder>() {
                b.append_value(&v);
            }
        }
        AvroValue::Double(v) => {
            if let Some(b) = builder.as_any_mut().downcast_mut::<Float64Builder>() {
                b.append_value(v);
            }
        }
        AvroValue::Int(v) => {
            if let Some(b) = builder.as_any_mut().downcast_mut::<Int32Builder>() {
                b.append_value(v);
            }
        }
        AvroValue::Float(v) => {
            if let Some(b) = builder.as_any_mut().downcast_mut::<Float32Builder>() {
                b.append_value(v);
            }
        }
        AvroValue::Boolean(v) => {
            if let Some(b) = builder.as_any_mut().downcast_mut::<BooleanBuilder>() {
                b.append_value(v);
            }
        }
        AvroValue::Null => {
            if let Some(b) = builder.as_any_mut().downcast_mut::<StringBuilder>() {
                b.append_null();
            }
        }
        _ => {}
    }
}

fn make_builder(dt: &DataType) -> Box<dyn ArrayBuilder> {
    match dt {
        DataType::Boolean => Box::new(BooleanBuilder::new()),
        DataType::Int32 => Box::new(Int32Builder::new()),
        DataType::Int64 => Box::new(Int64Builder::new()),
        DataType::Float32 => Box::new(Float32Builder::new()),
        DataType::Float64 => Box::new(Float64Builder::new()),
        DataType::Binary => Box::new(BinaryBuilder::new()),
        DataType::Utf8 => Box::new(StringBuilder::new()),
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            Box::new(TimestampMillisecondBuilder::new())
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            Box::new(TimestampMicrosecondBuilder::new())
        }
        _ => Box::new(StringBuilder::new()),
    }
}

// ============================================================================
// Benchmark 1: convert vs baseline (the headline number)
// ============================================================================

fn bench_convert_vs_baseline(c: &mut Criterion) {
    let mut group = c.benchmark_group("convert_vs_baseline");
    let record_count = 1000usize;

    for field_count in [5, 10, 20] {
        let (schema_json, avro_schema, records) = make_test_data(field_count, record_count, 64);
        let converter = RecordConverter::new(&schema_json, SchemaId(1)).unwrap();
        let arrow_schema = converter.arrow_schema().clone();

        group.throughput(Throughput::Elements(record_count as u64));

        group.bench_with_input(
            BenchmarkId::new("new", format!("{field_count}f")),
            &records,
            |b, records| {
                b.iter(|| {
                    converter
                        .convert(records, SchemaId(1), 0..record_count as u64, 0, Utc::now())
                        .unwrap()
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("old", format!("{field_count}f")),
            &records,
            |b, records| {
                b.iter(|| baseline_convert(records, &avro_schema, &arrow_schema, field_count));
            },
        );
    }

    group.finish();
}

// ============================================================================
// Benchmark 2: batch size scaling
// ============================================================================

fn bench_convert_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("convert_throughput");
    let field_count = 10;
    let string_len = 64;

    for record_count in [10, 100, 1000, 10_000] {
        let (schema_json, _, records) = make_test_data(field_count, record_count, string_len);
        let converter = RecordConverter::new(&schema_json, SchemaId(1)).unwrap();

        group.throughput(Throughput::Elements(record_count as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(record_count),
            &records,
            |b, records| {
                b.iter(|| {
                    converter
                        .convert(records, SchemaId(1), 0..record_count as u64, 0, Utc::now())
                        .unwrap()
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// Benchmark 3: payload size (string-heavy vs numeric-heavy)
// ============================================================================

fn bench_convert_payload_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("convert_payload_size");
    let record_count = 1000usize;

    let configs: Vec<(&str, usize, usize)> = vec![
        ("5f_short", 5, 16),
        ("10f_medium", 10, 128),
        ("20f_long", 20, 512),
    ];

    for (label, field_count, string_len) in configs {
        let (schema_json, _, records) = make_test_data(field_count, record_count, string_len);
        let converter = RecordConverter::new(&schema_json, SchemaId(1)).unwrap();

        let total_bytes: u64 = records.iter().map(|r| r.value.len() as u64).sum();
        group.throughput(Throughput::Bytes(total_bytes));

        group.bench_with_input(
            BenchmarkId::from_parameter(label),
            &records,
            |b, records| {
                b.iter(|| {
                    converter
                        .convert(records, SchemaId(1), 0..record_count as u64, 0, Utc::now())
                        .unwrap()
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// Benchmark 4: schema evolution (identity vs evolved with defaults)
// ============================================================================

fn bench_convert_schema_evolution(c: &mut Criterion) {
    let mut group = c.benchmark_group("convert_schema_evolution");
    let record_count = 1000usize;
    let string_len = 64;

    // v1: 8 fields
    let v1_json = make_schema_json(8);
    let v1_avro = AvroSchema::parse_str(&v1_json.to_string()).unwrap();

    // v2: v1 + 2 new string fields with defaults
    let mut v2_fields: Vec<serde_json::Value> = v1_json["fields"].as_array().unwrap().clone();
    v2_fields.push(serde_json::json!({
        "name": "new_field_a", "type": "string", "default": "default_a"
    }));
    v2_fields.push(serde_json::json!({
        "name": "new_field_b", "type": "string", "default": "default_b"
    }));
    let v2_json = serde_json::json!({
        "type": "record",
        "name": "BenchRecord",
        "fields": v2_fields
    });

    // Encode v1 records
    let mut rng = rand::thread_rng();
    let v1_records: Vec<Record> = (0..record_count)
        .map(|_| {
            let val = make_avro_record(&mut rng, 8, string_len);
            let bytes = to_avro_datum(&v1_avro, val).unwrap();
            Record::new(Bytes::from(bytes))
        })
        .collect();

    // Encode v2 records (for identity benchmark): v1 fields + 2 new fields
    let v2_avro = AvroSchema::parse_str(&v2_json.to_string()).unwrap();
    let v2_records: Vec<Record> = (0..record_count)
        .map(|_| {
            let AvroValue::Record(mut fields) = make_avro_record(&mut rng, 8, string_len) else {
                unreachable!()
            };
            fields.push((
                "new_field_a".into(),
                AvroValue::String(random_string(&mut rng, string_len)),
            ));
            fields.push((
                "new_field_b".into(),
                AvroValue::String(random_string(&mut rng, string_len)),
            ));
            let bytes = to_avro_datum(&v2_avro, AvroValue::Record(fields)).unwrap();
            Record::new(Bytes::from(bytes))
        })
        .collect();

    // Converter with v2 as reader, v1 registered as writer
    let mut converter = RecordConverter::new(&v2_json, SchemaId(2)).unwrap();
    converter
        .register_writer_schema(SchemaId(1), &v1_json)
        .unwrap();

    group.throughput(Throughput::Elements(record_count as u64));

    // Identity: v2 records read with v2 schema
    group.bench_function("identity", |b| {
        b.iter(|| {
            converter
                .convert(
                    &v2_records,
                    SchemaId(2),
                    0..record_count as u64,
                    0,
                    Utc::now(),
                )
                .unwrap()
        });
    });

    // Evolved: v1 records read with v2 schema (2 defaults filled)
    group.bench_function("evolved_2_defaults", |b| {
        b.iter(|| {
            converter
                .convert(
                    &v1_records,
                    SchemaId(1),
                    0..record_count as u64,
                    0,
                    Utc::now(),
                )
                .unwrap()
        });
    });

    group.finish();
}

// ============================================================================
// Criterion config & entry point
// ============================================================================

fn config() -> Criterion {
    Criterion::default()
        .sample_size(100)
        .measurement_time(Duration::from_secs(5))
        .warm_up_time(Duration::from_secs(1))
}

criterion_group! {
    name = benches;
    config = config();
    targets =
        bench_convert_vs_baseline,
        bench_convert_throughput,
        bench_convert_payload_size,
        bench_convert_schema_evolution,
}
criterion_main!(benches);
