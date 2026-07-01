-- Iceberg ingestion tracking with claim-based concurrency control.
--
-- Catch-up workers INSERT a pending claim before processing, then mark
-- it completed after the Iceberg commit succeeds.  A reaper expires
-- stale pending claims so another worker can retry.

CREATE TABLE iceberg_claims (
    batch_id   BIGINT       NOT NULL,
    ingest_time TIMESTAMPTZ NOT NULL,
    status     VARCHAR(16)  NOT NULL DEFAULT 'pending',  -- 'pending' | 'completed'
    claimed_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    PRIMARY KEY (batch_id, ingest_time)
) PARTITION BY RANGE (ingest_time);

-- Match topic_batches partitioning (daily).
-- Auto-create via same mechanism as topic_batches.
DO $$
DECLARE
    start_date DATE := CURRENT_DATE;
    end_date DATE;
    partition_name TEXT;
BEGIN
    FOR i IN 0..7 LOOP
        end_date := start_date + INTERVAL '1 day';
        partition_name := 'iceberg_claims_' || TO_CHAR(start_date, 'YYYYMMDD');

        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF iceberg_claims
             FOR VALUES FROM (%L) TO (%L)',
            partition_name,
            start_date,
            end_date
        );

        start_date := end_date;
    END LOOP;
END $$;

-- Index for finding stale pending claims (reaper query).
CREATE INDEX idx_iceberg_claims_pending
ON iceberg_claims (status, claimed_at)
WHERE status = 'pending';

-- Function to create daily partitions (same pattern as topic_batches).
CREATE OR REPLACE FUNCTION create_iceberg_claims_partition(target_date DATE)
RETURNS VOID AS $$
DECLARE
    partition_name TEXT;
    start_ts TIMESTAMPTZ;
    end_ts TIMESTAMPTZ;
BEGIN
    partition_name := 'iceberg_claims_' || TO_CHAR(target_date, 'YYYYMMDD');
    start_ts := target_date::TIMESTAMPTZ;
    end_ts := (target_date + INTERVAL '1 day')::TIMESTAMPTZ;

    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF iceberg_claims
         FOR VALUES FROM (%L) TO (%L)',
        partition_name,
        start_ts,
        end_ts
    );
END;
$$ LANGUAGE plpgsql;
