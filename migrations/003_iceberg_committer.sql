-- Single-committer-per-table election for Iceberg ingestion.
--
-- Any broker or worker may hold a table's lease; it renews on heartbeat
-- and expires like reader-group leases. The epoch increments on every
-- takeover and is stamped into each Iceberg snapshot
-- (flux.committer-epoch) so a paused ex-committer is fenced: it aborts
-- when it observes a snapshot with a newer epoch than its own lease.

CREATE TABLE iceberg_committers (
    table_ident  TEXT        PRIMARY KEY,
    holder       TEXT        NOT NULL,
    epoch        BIGINT      NOT NULL DEFAULT 1,
    lease_expiry TIMESTAMPTZ NOT NULL
);
