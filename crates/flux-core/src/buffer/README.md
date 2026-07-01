# Buffer Module

High-performance Avro to Parquet conversion service.

## Architecture

```
HTTP Request (Avro) → Schema Validation → Arrow RecordBatch → Parquet File
```

**Components:**
- `avro_converter` - Avro schema/value to Arrow conversion
- `parquet_writer` - Parquet file writing with configurable compression
- `service` - HTTP endpoints via Axum
- `error` - Unified error types

## Usage

```rust
use buffer::{build_router, AppState, ParquetWriterConfig};

let config = ParquetWriterConfig::default().with_output_dir("./output");
let state = Arc::new(AppState::new(config)?);
let app = build_router(state);
```

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check |
| POST | `/schema` | Register Avro schema |
| GET | `/schema/:name` | Get registered schema |
| POST | `/write/:schema_name` | Write raw Avro binary |
| POST | `/write-raw` | Write Avro container file |

## Example

```bash
# Register schema
curl -X POST localhost:8080/schema \
  -H "Content-Type: application/json" \
  -d '{"name":"user","schema":"{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}"}'

# Write Avro container
curl -X POST localhost:8080/write-raw --data-binary @data.avro
```
