-- Consolidated initial schema migration (squashed from 001..006).
-- System is pre-production; this file is the single migration authority.

-- Topics table
CREATE TABLE topics (
    topic_id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    retention_hours INT NOT NULL DEFAULT 168,  -- 7 days default
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for name lookups
CREATE INDEX idx_topics_name ON topics (name);

-- Topic offsets: tracks the next offset for each topic
CREATE TABLE topic_offsets (
    topic_id INT PRIMARY KEY REFERENCES topics(topic_id) ON DELETE CASCADE,
    next_offset BIGINT NOT NULL DEFAULT 0
);

-- Function to auto-create topic offset row when a topic is created
CREATE OR REPLACE FUNCTION create_topic_offset()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO topic_offsets (topic_id, next_offset)
    VALUES (NEW.topic_id, 0);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_create_topic_offset
AFTER INSERT ON topics
FOR EACH ROW
EXECUTE FUNCTION create_topic_offset();

-- Topic batches: segment index (partitioned by ingest_time for efficient retention)
CREATE TABLE topic_batches (
    batch_id BIGSERIAL,
    topic_id INT NOT NULL,
    schema_id INT NOT NULL,
    start_offset BIGINT NOT NULL,
    end_offset BIGINT NOT NULL,
    record_count INT NOT NULL,
    s3_key VARCHAR(512) NOT NULL,
    byte_offset BIGINT NOT NULL,
    byte_length BIGINT NOT NULL,
    ingest_time TIMESTAMPTZ NOT NULL,
    crc32 BIGINT NOT NULL,
    PRIMARY KEY (batch_id, ingest_time)
) PARTITION BY RANGE (ingest_time);

-- Create initial partition for today and next 7 days
DO $$
DECLARE
    start_date DATE := CURRENT_DATE;
    end_date DATE;
    partition_name TEXT;
BEGIN
    FOR i IN 0..7 LOOP
        end_date := start_date + INTERVAL '1 day';
        partition_name := 'topic_batches_' || TO_CHAR(start_date, 'YYYYMMDD');

        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF topic_batches
             FOR VALUES FROM (%L) TO (%L)',
            partition_name,
            start_date,
            end_date
        );

        start_date := end_date;
    END LOOP;
END $$;

-- Index for read queries: find batches by topic/offset
CREATE INDEX idx_topic_batches_fetch
ON topic_batches (topic_id, end_offset, ingest_time);

-- Index for S3 key lookups (used by GC)
CREATE INDEX idx_topic_batches_s3_key
ON topic_batches (s3_key);

-- Function to create daily partitions (run via cron/pg_cron)
CREATE OR REPLACE FUNCTION create_topic_batches_partition(target_date DATE)
RETURNS VOID AS $$
DECLARE
    partition_name TEXT;
    start_ts TIMESTAMPTZ;
    end_ts TIMESTAMPTZ;
BEGIN
    partition_name := 'topic_batches_' || TO_CHAR(target_date, 'YYYYMMDD');
    start_ts := target_date::TIMESTAMPTZ;
    end_ts := (target_date + INTERVAL '1 day')::TIMESTAMPTZ;

    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF topic_batches
         FOR VALUES FROM (%L) TO (%L)',
        partition_name,
        start_ts,
        end_ts
    );
END;
$$ LANGUAGE plpgsql;

-- Writer state: tracks last committed sequence for deduplication
CREATE TABLE writer_state (
    writer_id UUID PRIMARY KEY,
    last_seq BIGINT NOT NULL,
    last_acks JSONB NOT NULL,  -- Array of SegmentAck for last commit
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for cleanup of stale writers
CREATE INDEX idx_writer_state_updated_at
ON writer_state (updated_at);

-- Function to clean up stale writer state (run via cron)
CREATE OR REPLACE FUNCTION cleanup_stale_writers(retention_hours INT DEFAULT 24)
RETURNS INT AS $$
DECLARE
    deleted_count INT;
BEGIN
    DELETE FROM writer_state
    WHERE updated_at < NOW() - (retention_hours || ' hours')::INTERVAL;

    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Schema registry: stores Avro schemas with content-addressed deduplication

-- Schemas table: immutable, content-addressed by hash
CREATE TABLE schemas (
    schema_id SERIAL PRIMARY KEY,
    schema_hash BYTEA UNIQUE NOT NULL,  -- SHA-256 of canonical schema
    schema_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Reserve IDs 1-99 for protocol schemas
ALTER SEQUENCE schemas_schema_id_seq RESTART WITH 100;

-- Topic-schema association: which schemas are valid for a topic
CREATE TABLE topic_schemas (
    topic_id INT NOT NULL REFERENCES topics(topic_id) ON DELETE CASCADE,
    schema_id INT NOT NULL REFERENCES schemas(schema_id) ON DELETE RESTRICT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (topic_id, schema_id)
);

-- Index for looking up schemas by topic in order of registration
CREATE INDEX idx_topic_schemas_by_time
ON topic_schemas (topic_id, created_at DESC);

-- Insert protocol schemas (reserved IDs 1-8)
INSERT INTO schemas (schema_id, schema_hash, schema_json) VALUES
(1, decode('0000000000000000000000000000000000000000000000000000000000000001', 'hex'),
   '{"type":"record","name":"AppendRequest","fields":[]}'),
(2, decode('0000000000000000000000000000000000000000000000000000000000000002', 'hex'),
   '{"type":"record","name":"AppendResponse","fields":[]}'),
(3, decode('0000000000000000000000000000000000000000000000000000000000000003', 'hex'),
   '{"type":"record","name":"ReadRequest","fields":[]}'),
(4, decode('0000000000000000000000000000000000000000000000000000000000000004', 'hex'),
   '{"type":"record","name":"ReadResponse","fields":[]}'),
(5, decode('0000000000000000000000000000000000000000000000000000000000000005', 'hex'),
   '{"type":"record","name":"Error","fields":[]}'),
(6, decode('0000000000000000000000000000000000000000000000000000000000000006', 'hex'),
   '{"type":"record","name":"Ping","fields":[]}'),
(7, decode('0000000000000000000000000000000000000000000000000000000000000007', 'hex'),
   '{"type":"record","name":"Pong","fields":[]}'),
(8, decode('0000000000000000000000000000000000000000000000000000000000000008', 'hex'),
   '{"type":"record","name":"RateLimit","fields":[]}');

-- Reset sequence to start user schemas at 100
SELECT setval('schemas_schema_id_seq', 100, false);

-- Reader groups: coordination for parallel consumption

-- Reader groups table: one row per (group, topic) pair
CREATE TABLE reader_groups (
    group_id VARCHAR(255) NOT NULL,
    topic_id INT NOT NULL REFERENCES topics(topic_id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (group_id, topic_id)
);

-- Reader members: active readers in a group
CREATE TABLE reader_members (
    group_id VARCHAR(255) NOT NULL,
    topic_id INT NOT NULL,
    reader_id VARCHAR(255) NOT NULL,
    broker_id UUID NOT NULL,  -- Which broker this reader is connected to
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (group_id, topic_id, reader_id),
    FOREIGN KEY (group_id, topic_id) REFERENCES reader_groups(group_id, topic_id) ON DELETE CASCADE
);

-- Index for finding expired members
CREATE INDEX idx_reader_members_heartbeat
ON reader_members (last_heartbeat);

-- Reader group state: dispatch cursor and committed watermark per (group, topic)
CREATE TABLE reader_group_state (
    group_id VARCHAR(255) NOT NULL,
    topic_id INT NOT NULL,
    dispatch_cursor BIGINT NOT NULL DEFAULT 0,
    committed_watermark BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (group_id, topic_id),
    FOREIGN KEY (group_id, topic_id) REFERENCES reader_groups(group_id, topic_id) ON DELETE CASCADE
);

-- Reader inflight: offset ranges currently being processed by readers
CREATE TABLE reader_inflight (
    group_id VARCHAR(255) NOT NULL,
    topic_id INT NOT NULL,
    start_offset BIGINT NOT NULL,
    end_offset BIGINT NOT NULL,
    reader_id VARCHAR(255) NOT NULL,
    lease_expires_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (group_id, topic_id, start_offset),
    FOREIGN KEY (group_id, topic_id) REFERENCES reader_groups(group_id, topic_id) ON DELETE CASCADE,
    CHECK (end_offset > start_offset)
);

-- Index for finding expired leases
CREATE INDEX idx_reader_inflight_lease
ON reader_inflight (lease_expires_at)
WHERE lease_expires_at IS NOT NULL;

-- Index for finding inflight by reader
CREATE INDEX idx_reader_inflight_reader
ON reader_inflight (group_id, topic_id, reader_id);

-- Authentication and authorization tables

-- API keys for machine-to-machine authentication
CREATE TABLE api_keys (
    key_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    key_hash BYTEA NOT NULL,  -- bcrypt hash of the API key
    name VARCHAR(255) NOT NULL,
    principal VARCHAR(255) NOT NULL,  -- The identity this key represents
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ,
    last_used_at TIMESTAMPTZ
);

-- Index for key lookups
CREATE INDEX idx_api_keys_principal ON api_keys (principal);

-- Access control lists
CREATE TABLE acls (
    acl_id SERIAL PRIMARY KEY,
    principal VARCHAR(255) NOT NULL,  -- User or service identity
    resource_type VARCHAR(50) NOT NULL,  -- 'topic', 'group', 'cluster'
    resource_name VARCHAR(255) NOT NULL,  -- Specific resource or '*' for wildcard
    operation VARCHAR(50) NOT NULL,  -- 'produce', 'consume', 'admin', etc.
    permission VARCHAR(10) NOT NULL CHECK (permission IN ('allow', 'deny')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for ACL lookups
CREATE INDEX idx_acls_principal ON acls (principal, resource_type, resource_name);

-- Unique constraint to prevent duplicate ACLs
CREATE UNIQUE INDEX idx_acls_unique
ON acls (principal, resource_type, resource_name, operation);

-- Insert default admin ACL (allow admin principal to do anything)
INSERT INTO acls (principal, resource_type, resource_name, operation, permission)
VALUES ('admin', 'cluster', '*', 'admin', 'allow');
