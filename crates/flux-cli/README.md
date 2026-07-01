# flux-cli

CLI tool for interacting with a running Flux cluster.

## Local Setup

### 1. Start PostgreSQL

```bash
cd docker
docker-compose up -d
```

This starts PostgreSQL on port 5433 and auto-applies migrations.

### 2. Start the broker

```bash
export DATABASE_URL=postgres://postgres:postgres@localhost:5433/flux
cargo run -p flux-broker --bin flux-broker
```

The broker starts two servers:
- WebSocket on `127.0.0.1:9000` (data plane: write/read)
- Admin HTTP on `127.0.0.1:9001` (management: topics, schemas, ACLs)

### 3. Bootstrap an API key

The admin API requires authentication. Bootstrap creates an admin key directly in the database:

```bash
export DATABASE_URL=postgres://postgres:postgres@localhost:5433/flux
cargo run -p flux-cli -- bootstrap
```

This prints a key like `tb_<uuid>_<secret>`. Export it:

```bash
export FLUX_API_KEY=tb_...
```

### 4. Use the CLI

```bash
# Create a topic
flux topic create my-events --partitions 3

# Write a record (--topic accepts name or numeric ID)
flux write --topic my-events '{"name":"alice"}'

# Write with explicit key and schema
flux write --topic my-events --key user-123 --schema-id 1 '{"name":"alice"}'

# Pipe from stdin
echo '{"name":"bob"}' | flux write --topic my-events

# Live-tail with TUI
flux tail --topic my-events

# Tail as JSON with offset range
flux tail --topic my-events --start 0:0 --end 0:10 --output json
```

## Commands

### `bootstrap`
Create an admin API key directly via the database. Requires `DATABASE_URL`.

### `topic`
```bash
flux topic list
flux topic create my-topic --partitions 3
flux topic get 1
flux topic update 1 --retention-hours 48
flux topic delete 1
```

### `schema`
```bash
flux schema register --topic-id 1 --file schema.avsc
flux schema get 100
flux schema list --topic-id 1
flux schema check --topic-id 1 --file schema.avsc
```

### `write`
```bash
flux write --topic my-events '{"name":"alice"}'
flux write --topic 1 --key user-123 --schema-id 100 '{"name":"alice"}'
echo '{"data":"value"}' | flux write --topic my-events
```

`--schema-id` defaults to the latest schema registered for the topic.

### `tail`
```bash
flux tail --topic my-events                     # TUI (default)
flux tail --topic my-events --output json       # JSONL to stdout
flux tail --topic 1 --start 0:0,1:0 --end 0:100,1:100 --output json
```

- `--start` / `--end`: `partition:offset,...` format
- `--output`: `tui` (default) or `json`
- TUI: press `q` or `Esc` to quit

## Configuration

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--admin-url` | | `http://localhost:9001` | Admin API URL |
| `--ws-url` | | `ws://localhost:9000` | WebSocket URL |
| `--api-key` | `FLUX_API_KEY` | | API key for authentication |

## Architecture

- `bootstrap.rs` — Create admin API key via direct database access
- `client.rs` — HTTP client wrapping admin + schema registry APIs
- `topic.rs` — Topic CRUD commands
- `schema.rs` — Schema register/get/list/check commands
- `write.rs` — Record append via SDK Writer (WebSocket)
- `tail.rs` — Live-tail via SDK Reader with TUI or JSON output
