# Why we built Flux

Flux combines pub/sub's simple consume model, Bufstream-style stateless brokers backed by S3, and built-in Iceberg integration. It trades partition-based ordering for lower operational overhead and cost.

## Broker-side dispatch instead of partitions

Kafka assigns partitions to consumers. When a consumer dies, the group rebalances. When you pick the wrong partition count, you live with it or migrate.

Flux doesn't have partitions. The broker maintains a single offset sequence per topic and dispatches offset ranges to consumers on demand. Ranges are leased — if a consumer doesn't commit within 45 seconds, the broker hands the range to someone else. No rebalancing protocol, no group coordinator, no partition topology to manage.

Flux guarantees causal ordering (if write A completes before B starts, A gets a lower offset) and per-producer ordering, but not per-key ordering.
## S3 instead of local disks

Kafka brokers store segments on local disks and replicate them to follower brokers. Replication factor 3 is standard, so you store everything three times. EBS gp3 costs $0.08/GB/month. For 6 TB of data with RF=3, that's 18 TB at $1,440/month.

Flux writes directly to S3. S3 costs $0.023/GB/month and handles durability internally (11 nines). Same 6 TB costs $138/month.

The broker buffers records in memory and flushes to S3 every 100ms. Multiple producers' records are merged into a single S3 PUT — regardless of how many producers are active, you get roughly 10 PUTs per second. Each flush atomically commits batch metadata and dedup state to Postgres in a single transaction. If the PUT succeeds but the Postgres commit fails, the data is orphaned in S3 (harmless). If the commit succeeds, the batch is visible to readers.

## Concrete cost comparison

Flux benchmarks at 5M events/sec on 8 cores. For this comparison we match core count at 32 vCPUs across all three setups. Workload: 100,000 events/sec, 1 KB average event size, 7-day retention, AWS us-east-1:

| | Self-managed Kafka | Amazon MSK | Flux |
|---|---|---|---|
| **Brokers** | 8× m6i.xlarge (32 vCPUs) — $1,120/mo | 8× kafka.m5.xlarge (32 vCPUs) — $1,220/mo | 1× c6g.8xlarge (32 vCPUs) — $800/mo |
| **Storage** | 180 TB EBS gp3 (3× RF) — $14,400/mo | 180 TB provisioned (3× RF) — $18,000/mo | 60 TB S3 — $1,380/mo |
| **S3 requests** | — | — | PUTs + GETs — $200/mo |
| **Coordination** | ZooKeeper (3 nodes) — $90/mo | Included | Postgres (RDS) — $50/mo |
| **Schema registry** | Confluent SR — $30/mo | Glue (free) | Built in |
| **Iceberg ingestion** | Kafka Connect (4 workers) — $240/mo | MSK Connect (4 MCU) — $320/mo | Built in |
| **Total** | **~$15,900/mo** | **~$19,500/mo** | **~$2,430/mo** |

With matched core counts, compute costs are comparable ($1,120 vs $800). The gap is almost entirely storage: no 3× replication ($14,400 → $1,380) and no Iceberg connector infrastructure ($240–$320 → $0).

At higher throughput, the ratio widens further. Kafka needs linearly more brokers and disks; Flux's S3 costs scale with data volume but compute stays flat.

## Schemas in code, not in a registry

Confluent Schema Registry is a separate service with its own deployment, monitoring, and failure modes. Flux integrates schema management into the broker. You annotate your types with schema metadata (namespace, renames, deletions) and the SDK infers an Avro schema from the type structure. The broker validates compatibility on registration.

The practical difference: schema history lives in your code and your git history, not in a separate service's database.

## Iceberg without connectors

Getting data from Kafka to Iceberg means running Kafka Connect workers, an Iceberg sink connector, connector configuration, and monitoring for each. The broker's flush loop already has the data and the schema, so Flux writes directly to Iceberg tables during flush. Schema changes propagate automatically. A background catch-up path backfills any missed batches.

## Tradeoffs

- **Per-key ordering.** Kafka's partition model gives per-key ordering through partition hashing. Flux has causal and per-producer ordering but not per-key.
- **Ecosystem breadth.** Kafka has hundreds of connectors, mature monitoring integrations, and 13 years of operational knowledge. Flux is new.
- **Multi-datacenter replication.** Kafka has MirrorMaker. Flux relies on S3 cross-region replication.
- **Production hours.** Kafka is battle-tested at enormous scale. Flux is new.

If you want fewer moving parts, lower cost, and you're building on cloud infrastructure with idempotent consumers, [try Flux](../docs/).
