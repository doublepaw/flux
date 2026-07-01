# Avro Module

High-performance Avro deserialization with zero-copy support.

## Performance

**5-9x faster than apache-avro** for Value deserialization:

| Payload | apache-avro | flux | Speedup |
|---------|-------------|---------|---------|
| small   | 3.05 µs     | 341 ns  | 8.9x    |
| medium  | 6.39 µs     | 1.04 µs | 6.1x    |
| large   | 14.59 µs    | 2.88 µs | 5.1x    |

Throughput: **1.5-1.6 GiB/s**

## Features

- **Zero-copy deserialization**: Strings and bytes borrow directly from input
- **Bump allocation**: Arena-based allocation for batch processing
- **Schema-driven**: Pre-computed schema graph with O(1) field lookups

## Architecture

```
avro/
├── schema/           # Schema parsing and representation
│   ├── safe/         # Safe schema building (SchemaMut)
│   └── self_referential.rs  # Frozen schema with cyclic references
├── de/               # Deserialization
│   ├── deserializer/ # Serde Deserializer implementation
│   └── read/         # Low-level reading (varint, slices)
└── value/            # Dynamic value types
    ├── bump.rs       # Arena-allocated BumpValue
    └── record.rs     # GenericRecord with field index
```

## Usage

### Batch Deserialization with Bump Allocation

For high-throughput scenarios, use `BatchDeserializer` with bump allocation:

```rust
use flux::avro::Schema;
use flux::avro::value::{BatchDeserializer, BumpValue};

let schema: Schema = r#"{"type": "record", "name": "User", "fields": [
    {"name": "id", "type": "long"},
    {"name": "name", "type": "string"}
]}"#.parse()?;

let mut deserializer = BatchDeserializer::new(&schema);

for payload in payloads {
    deserializer.reset();
    let value = deserializer.deserialize(payload)?;

    // Access fields by index (O(1))
    if let BumpValue::Record(fields) = value {
        let id = &fields[0];    // BumpValue::Long
        let name = &fields[1];  // BumpValue::String
    }
    // Memory is reused on next reset()
}
```

### Single Value Deserialization

```rust
use flux::avro::Schema;
use flux::avro::value::{from_datum_slice_bump, BumpValue};

let schema: Schema = r#"{"type": "record", "name": "User", "fields": [
    {"name": "id", "type": "long"},
    {"name": "name", "type": "string"}
]}"#.parse()?;

let bump = bumpalo::Bump::new();
let value = from_datum_slice_bump(&bytes, &schema, &bump)?;

if let BumpValue::Record(fields) = value {
    // fields[0] is id, fields[1] is name
}
```

## Key Optimizations

1. **Varint decoding**: Inline fast path for 1-3 byte varints
2. **Bump allocation**: Arena allocator eliminates per-value allocation overhead
3. **Pre-computed schema**: Schema graph with cached field indices and union lookups
4. **Zero-copy strings**: Borrows directly from input slice when possible
5. **Inline hot paths**: Critical deserializer paths are `#[inline(always)]`

## Modules

| Module | Purpose |
|--------|---------|
| `schema` | Schema parsing, validation, and representation |
| `de` | Deserialization via serde `Deserializer` trait |
| `value` | Dynamic `BumpValue` and `Value` types |
