// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Write Arrow batches to Iceberg tables via fast_append.
//!
//! One Iceberg table per Flux topic. Tables are auto-created on
//! first flush and evolved when schema changes are detected.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::record_batch::RecordBatch as ArrowRecordBatch;
use iceberg::spec::DataFileFormat;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::IcebergWriter as _;
use iceberg::writer::IcebergWriterBuilder;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use parquet::file::properties::WriterProperties;
use sqlx::PgPool;
use tokio::sync::RwLock;
use tracing::{debug, info};

use flux_common::ids::{SchemaId, TopicId};

use crate::committer;
use crate::config::IcebergConfig;
use crate::error::{IcebergError, Result};
use crate::iceberg_buffer::CommittedSegment;
use crate::record_converter::RecordConverter;
use crate::schema_mapping;
use crate::tracking;

/// Target parquet file size before rolling (256 MB).
const TARGET_FILE_SIZE: usize = 256 * 1024 * 1024;

/// Snapshot summary property: uuid of the flux commit that produced it.
const PROP_COMMIT_ID: &str = "flux.commit-id";
/// Snapshot summary property: JSON map of topic_id → max ingested
/// batch_id, carried forward cumulatively. The snapshot itself is the
/// source of truth for ingestion progress: data and bookkeeping commit
/// atomically, so a crash between the Iceberg commit and Postgres
/// mark_completed re-claims batches that the next commit then skips.
const PROP_WATERMARKS: &str = "flux.watermarks";
/// Snapshot summary property: committer lease epoch that produced it.
const PROP_COMMITTER_EPOCH: &str = "flux.committer-epoch";

/// Manages Iceberg table writes for all topics.
pub struct TableWriter {
    catalog: Arc<dyn Catalog>,
    config: IcebergConfig,
    pool: PgPool,
    /// This writer's identity for committer-lease election.
    holder: String,
    /// Topic name cache: TopicId → topic name.
    topic_names: RwLock<HashMap<TopicId, String>>,
}

impl TableWriter {
    /// Create a new writer backed by a SQL catalog.
    pub async fn new(config: IcebergConfig, pool: PgPool) -> Result<Arc<Self>> {
        let catalog = create_catalog(&config).await?;

        // Ensure namespace exists
        let ns = NamespaceIdent::new(config.namespace.clone());
        if catalog.get_namespace(&ns).await.is_err() {
            let _ = catalog
                .create_namespace(&ns, HashMap::new())
                .await
                .map_err(|e| IcebergError::Iceberg(format!("create namespace: {e}")));
        }

        Ok(Arc::new(Self {
            catalog,
            config,
            pool,
            holder: uuid::Uuid::new_v4().to_string(),
            topic_names: RwLock::new(HashMap::new()),
        }))
    }

    /// Write committed segments for a single topic to Iceberg.
    pub async fn write_segments(
        &self,
        topic_id: TopicId,
        segments: Vec<CommittedSegment>,
    ) -> Result<()> {
        if segments.is_empty() {
            return Ok(());
        }

        let table_name = self.resolve_topic_name(topic_id).await?;
        let table_ident = TableIdent::new(
            NamespaceIdent::new(self.config.namespace.clone()),
            table_name.clone(),
        );

        // Single committer per table: only the lease holder proceeds.
        // Batches stay unclaimed/pending on failure, so catch-up on the
        // actual committer re-ingests them.
        let lease_epoch = committer::acquire(
            &self.pool,
            &table_ident.to_string(),
            &self.holder,
            self.config.committer_lease_secs,
        )
        .await?
        .ok_or_else(|| IcebergError::NotCommitter(table_ident.to_string()))?;

        // Get latest schema for this topic to build converter
        let (reader_json, reader_schema_id) = self.get_latest_schema(topic_id).await?;
        let avro_schema = apache_avro::Schema::parse(&reader_json)
            .map_err(|e| IcebergError::Schema(format!("parse avro schema: {e}")))?;

        let mut converter = RecordConverter::new(&reader_json, reader_schema_id)?;

        // Register all writer schemas we'll need
        for seg in &segments {
            if seg.schema_id != reader_schema_id
                && let Ok(writer_json) = self.get_schema_json(seg.schema_id).await
            {
                let _ = converter.register_writer_schema(seg.schema_id, &writer_json);
            }
        }

        // Ensure Iceberg table exists
        let table = self.ensure_table(&table_ident, &avro_schema).await?;

        // The current snapshot's watermark, not Postgres claims, decides
        // what is already ingested: drop anything at or below it instead
        // of writing duplicates (crash between commit and mark_completed
        // re-claims batches; this makes the retry a no-op).
        let (mut watermarks, snapshot_epoch) = read_snapshot_state(&table);
        if snapshot_epoch > lease_epoch {
            return Err(IcebergError::Fenced {
                table: table_ident.to_string(),
                snapshot_epoch,
                lease_epoch,
            });
        }
        let topic_key = topic_id.0.to_string();
        let watermark = watermarks.get(&topic_key).copied().unwrap_or(i64::MIN);
        // All claimed batches get marked complete below — including ones
        // dropped as already-ingested, else catch-up re-claims them forever.
        let claimed: Vec<(i64, chrono::DateTime<chrono::Utc>)> = segments
            .iter()
            .map(|s| (s.batch_id, s.ingest_time))
            .collect();
        let mut segments: Vec<CommittedSegment> = segments
            .into_iter()
            .filter(|s| s.batch_id > watermark)
            .collect();
        // Watermark soundness needs per-topic batch_id order (batch_ids
        // and offsets are assigned in the same transaction, so they
        // agree); sort in case hot-path and catch-up batches interleave.
        segments.sort_unstable_by_key(|s| s.batch_id);

        if !segments.is_empty() {
            // Convert segments to Arrow batches
            let mut arrow_batches = Vec::new();
            for seg in &segments {
                let batch = converter.convert(
                    &seg.records,
                    seg.schema_id,
                    seg.start_offset..seg.end_offset,
                    topic_id.0 as i32,
                    seg.ingest_time,
                )?;
                arrow_batches.push(batch);
            }

            let max_batch_id = segments.last().map(|s| s.batch_id).unwrap_or(watermark);
            watermarks.insert(topic_key, max_batch_id.max(watermark));
            let properties = HashMap::from([
                (PROP_COMMIT_ID.to_string(), uuid::Uuid::new_v4().to_string()),
                (
                    PROP_WATERMARKS.to_string(),
                    serde_json::to_string(&watermarks)
                        .map_err(|e| IcebergError::Iceberg(format!("watermarks json: {e}")))?,
                ),
                (PROP_COMMITTER_EPOCH.to_string(), lease_epoch.to_string()),
            ]);

            self.write_arrow_batches(&table, arrow_batches, properties)
                .await?;
        }

        // Postgres claims are demoted to work distribution and progress
        // metrics; correctness lives in the snapshot watermark above.
        tracking::mark_completed(&self.pool, &claimed).await?;

        debug!(
            topic = table_name,
            segments = segments.len(),
            "iceberg flush committed"
        );

        Ok(())
    }

    /// Ensure the Iceberg table exists, creating it if needed.
    async fn ensure_table(
        &self,
        ident: &TableIdent,
        avro_schema: &apache_avro::Schema,
    ) -> Result<Table> {
        match self.catalog.load_table(ident).await {
            Ok(table) => Ok(table),
            Err(_) => {
                info!(table = %ident, "creating iceberg table");
                let (iceberg_schema, _) = schema_mapping::avro_to_iceberg_schema(avro_schema)?;

                let creation = TableCreation::builder()
                    .name(ident.name().to_string())
                    .schema(iceberg_schema)
                    .location(format!("{}/{}", self.config.warehouse, ident.name()))
                    .build();

                self.catalog
                    .create_table(ident.namespace(), creation)
                    .await
                    .map_err(|e| IcebergError::Iceberg(format!("create table: {e}")))
            }
        }
    }

    /// Write Arrow RecordBatches to an Iceberg table, committing one
    /// snapshot stamped with the given summary properties.
    async fn write_arrow_batches(
        &self,
        table: &Table,
        batches: Vec<ArrowRecordBatch>,
        snapshot_properties: HashMap<String, String>,
    ) -> Result<()> {
        let file_io = table.file_io().clone();

        let location_gen = DefaultLocationGenerator::new(table.metadata().clone())
            .map_err(|e| IcebergError::Iceberg(format!("location generator: {e}")))?;
        // Unique suffix per flush: the generator's counter restarts at 0
        // for every writer instance, so without it the second flush to a
        // table reuses iceberg-00000.parquet and the commit is rejected.
        let file_name_gen = DefaultFileNameGenerator::new(
            "iceberg".to_string(),
            Some(uuid::Uuid::new_v4().simple().to_string()),
            DataFileFormat::Parquet,
        );

        let iceberg_schema = table.metadata().current_schema().clone();

        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(Default::default()))
            .build();

        let parquet_builder = ParquetWriterBuilder::new(props, iceberg_schema.clone());

        let rolling_builder = RollingFileWriterBuilder::new(
            parquet_builder,
            TARGET_FILE_SIZE,
            file_io,
            location_gen,
            file_name_gen,
        );

        let mut writer = DataFileWriterBuilder::new(rolling_builder)
            .build(None)
            .await
            .map_err(|e| IcebergError::Iceberg(format!("build writer: {e}")))?;

        for batch in batches {
            let batch = align_field_ids(batch, &iceberg_schema)?;
            writer
                .write(batch)
                .await
                .map_err(|e| IcebergError::Iceberg(format!("write batch: {e}")))?;
        }

        let data_files = writer
            .close()
            .await
            .map_err(|e| IcebergError::Iceberg(format!("close writer: {e}")))?;

        if data_files.is_empty() {
            return Ok(());
        }

        // Commit via fast_append; the properties ride on the snapshot so
        // data and ingestion bookkeeping become atomic.
        let tx = Transaction::new(table);
        let action = tx
            .fast_append()
            .add_data_files(data_files)
            .set_snapshot_properties(snapshot_properties);
        let tx = action
            .apply(tx)
            .map_err(|e| IcebergError::Iceberg(format!("apply fast_append: {e}")))?;
        tx.commit(&*self.catalog)
            .await
            .map_err(|e| IcebergError::Iceberg(format!("commit: {e}")))?;

        Ok(())
    }

    /// Resolve topic_id → topic name from Postgres.
    async fn resolve_topic_name(&self, topic_id: TopicId) -> Result<String> {
        {
            let cache = self.topic_names.read().await;
            if let Some(name) = cache.get(&topic_id) {
                return Ok(name.clone());
            }
        }

        let name: String = sqlx::query_scalar("SELECT name FROM topics WHERE topic_id = $1")
            .bind(topic_id.0 as i32)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| IcebergError::Schema(format!("topic not found: {e}")))?;

        self.topic_names
            .write()
            .await
            .insert(topic_id, name.clone());
        Ok(name)
    }

    /// Get the latest Avro schema JSON for a topic.
    async fn get_latest_schema(&self, topic_id: TopicId) -> Result<(serde_json::Value, SchemaId)> {
        let row: (i32, serde_json::Value) = sqlx::query_as(
            r#"SELECT s.schema_id, s.schema_json
               FROM topic_schemas ts
               JOIN schemas s USING (schema_id)
               WHERE ts.topic_id = $1
               ORDER BY ts.created_at DESC
               LIMIT 1"#,
        )
        .bind(topic_id.0 as i32)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| IcebergError::Schema(format!("no schema for topic: {e}")))?;

        Ok((row.1, SchemaId(row.0 as u32)))
    }

    /// Get schema JSON by ID.
    async fn get_schema_json(&self, schema_id: SchemaId) -> Result<serde_json::Value> {
        let json: serde_json::Value =
            sqlx::query_scalar("SELECT schema_json FROM schemas WHERE schema_id = $1")
                .bind(schema_id.0 as i32)
                .fetch_one(&self.pool)
                .await?;
        Ok(json)
    }
}

/// Re-tag the batch's top-level PARQUET:field_id metadata with the ids
/// from the table's actual schema: the catalog renumbers field ids on
/// table creation, so the converter's pre-assigned ids may not match.
fn align_field_ids(
    batch: ArrowRecordBatch,
    schema: &iceberg::spec::Schema,
) -> Result<ArrowRecordBatch> {
    use arrow::datatypes::Schema as ArrowSchema;
    let fields: Vec<arrow::datatypes::Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| {
            let mut metadata = f.metadata().clone();
            if let Some(id) = schema.field_id_by_name(f.name()) {
                metadata.insert(
                    parquet::arrow::PARQUET_FIELD_ID_META_KEY.to_string(),
                    id.to_string(),
                );
            }
            f.as_ref().clone().with_metadata(metadata)
        })
        .collect();
    ArrowRecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), batch.columns().to_vec())
        .map_err(|e| IcebergError::Iceberg(format!("align field ids: {e}")))
}

/// Read (watermarks, committer epoch) from the table's current snapshot
/// summary. Missing snapshot or properties mean a fresh table: empty
/// watermarks, epoch 0.
fn read_snapshot_state(table: &Table) -> (HashMap<String, i64>, i64) {
    let Some(snapshot) = table.metadata().current_snapshot() else {
        return (HashMap::new(), 0);
    };
    let props = &snapshot.summary().additional_properties;
    let watermarks = props
        .get(PROP_WATERMARKS)
        .and_then(|json| serde_json::from_str(json).ok())
        .unwrap_or_default();
    let epoch = props
        .get(PROP_COMMITTER_EPOCH)
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    (watermarks, epoch)
}

/// Pick the storage backend from the warehouse URI scheme: the catalog
/// (SQL or REST) needs an explicit StorageFactory since iceberg-rust 0.9
/// moved cloud backends out of core.
fn storage_factory_for(warehouse: &str) -> Arc<dyn iceberg::io::StorageFactory> {
    use iceberg_storage_opendal::OpenDalStorageFactory;
    if warehouse.starts_with("s3://") || warehouse.starts_with("s3a://") {
        let scheme = warehouse.split("://").next().unwrap_or("s3").to_string();
        Arc::new(OpenDalStorageFactory::S3 {
            configured_scheme: scheme,
            customized_credential_load: None,
        })
    } else if warehouse.starts_with("gs://") {
        Arc::new(OpenDalStorageFactory::Gcs)
    } else {
        Arc::new(OpenDalStorageFactory::Fs)
    }
}

/// Create the catalog from config: REST (the engine-visible default for
/// deployments) or SQL (dev/tests).
async fn create_catalog(config: &IcebergConfig) -> Result<Arc<dyn Catalog>> {
    use iceberg::CatalogBuilder;

    let storage = storage_factory_for(&config.warehouse);
    match config.catalog_type.as_str() {
        "rest" => {
            use iceberg_catalog_rest::RestCatalogBuilder;
            let mut props = HashMap::from([
                ("uri".to_string(), config.catalog_uri.clone()),
                ("warehouse".to_string(), config.warehouse.clone()),
            ]);
            if let Ok(token) = std::env::var("ICEBERG_REST_TOKEN") {
                props.insert("token".to_string(), token);
            }
            let catalog = RestCatalogBuilder::default()
                .with_storage_factory(storage)
                .load("flux", props)
                .await
                .map_err(|e| IcebergError::Iceberg(format!("create rest catalog: {e}")))?;
            Ok(Arc::new(catalog))
        }
        "sql" => {
            use iceberg_catalog_sql::SqlCatalogBuilder;
            let catalog = SqlCatalogBuilder::default()
                .uri(config.catalog_uri.clone())
                .warehouse_location(config.warehouse.clone())
                .with_storage_factory(storage)
                .load("flux", HashMap::new())
                .await
                .map_err(|e| IcebergError::Iceberg(format!("create catalog: {e}")))?;
            Ok(Arc::new(catalog))
        }
        other => Err(IcebergError::Iceberg(format!(
            "unknown ICEBERG_CATALOG type: {other} (expected rest|sql)"
        ))),
    }
}
