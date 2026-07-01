# Cross-Language E2E Tests

This directory contains E2E tests that verify interoperability between the Rust broker and the Java/Python SDKs.

## Prerequisites

### Python SDK

```bash
cd sdks/python
pip install -e .
# or just: pip install websockets
```

### Java SDK

```bash
cd sdks/java/flux-sdk
mvn package -DskipTests
```

This creates `target/flux-sdk-0.1.0-jar-with-dependencies.jar`.

### Database

Start a PostgreSQL instance:

```bash
docker run -d --name flux-test-db \
  -e POSTGRES_PASSWORD=postgres \
  -p 5433:5432 \
  postgres:16
```

## Running Tests

### All cross-language tests

```bash
DATABASE_URL=postgres://postgres:postgres@localhost:5433 \
  cargo test --test cross_language_e2e -- --ignored
```

### Individual tests

```bash
# Python writer -> Python reader
DATABASE_URL=... cargo test --test cross_language_e2e test_python_to_python -- --ignored

# Java writer -> Java reader
DATABASE_URL=... cargo test --test cross_language_e2e test_java_to_java -- --ignored

# Java writer -> Python reader (cross-language)
DATABASE_URL=... cargo test --test cross_language_e2e test_java_to_python -- --ignored

# Python writer -> Java reader (cross-language)
DATABASE_URL=... cargo test --test cross_language_e2e test_python_to_java -- --ignored

# Mixed writers (Java + Python) -> single topic
DATABASE_URL=... cargo test --test cross_language_e2e test_mixed_writers -- --ignored
```

## Test Scripts

The test scripts can also be run manually for debugging:

### Python Writer

```bash
python3 python_writer.py ws://localhost:9000 1 0 5
```

### Python Reader

```bash
python3 python_reader.py ws://localhost:9000 1 0 5
```

### Java Writer

```bash
java -cp "../../sdks/java/flux-sdk/target/*:." \
  io.flux.test.JavaWriter ws://localhost:9000 1 0 5
```

### Java Reader

```bash
java -cp "../../sdks/java/flux-sdk/target/*:." \
  io.flux.test.JavaReader ws://localhost:9000 1 0 5
```

## Test Matrix

### Wire protocol tests

| Writer | Reader | Test Name |
|----------|----------|-----------|
| Python | Python | `test_python_to_python` |
| Java | Java | `test_java_to_java` |
| Java | Python | `test_java_to_python` |
| Python | Java | `test_python_to_java` |
| Mixed | Python | `test_mixed_writers` |

### Schema (Avro-encoded value) tests

| Writer | Reader | Test Name |
|----------|----------|-----------|
| Python | Java | `test_python_schema_to_java` |
| Java | Python | `test_java_schema_to_python` |

## What These Tests Verify

1. **Wire protocol compatibility**: Java and Python SDKs encode/decode messages correctly
2. **Record serialization**: Keys and values are preserved across languages
3. **Reader groups**: GroupReader in each SDK can join and read correctly
4. **Offset tracking**: Records are delivered at correct offsets
5. **Schema interop**: Avro-encoded record values are byte-compatible across SDKs
