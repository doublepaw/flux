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

use crate::config::IcebergConfig;
use crate::error::{IcebergError, Result};
use crate::iceberg_buffer::CommittedSegment;
use crate::record_converter::RecordConverter;
use crate::schema_mapping;
use crate::tracking;

/// Target parquet file size before rolling (256 MB).
const TARGET_FILE_SIZE: usize = 256 * 1024 * 1024;

/// Manages Iceberg table writes for all topics.
pub struct TableWriter {
    catalog: Arc<dyn Catalog>,
    config: IcebergConfig,
    pool: PgPool,
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

        // Write to Iceberg
        self.write_arrow_batches(&table, arrow_batches).await?;

        // Mark batches as ingested
        let batch_ids: Vec<(i64, chrono::DateTime<chrono::Utc>)> = segments
            .iter()
            .map(|s| (s.batch_id, s.ingest_time))
            .collect();
        tracking::mark_completed(&self.pool, &batch_ids).await?;

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

    /// Write Arrow RecordBatches to an Iceberg table.
    async fn write_arrow_batches(
        &self,
        table: &Table,
        batches: Vec<ArrowRecordBatch>,
    ) -> Result<()> {
        let file_io = table.file_io().clone();

        let location_gen = DefaultLocationGenerator::new(table.metadata().clone())
            .map_err(|e| IcebergError::Iceberg(format!("location generator: {e}")))?;
        let file_name_gen =
            DefaultFileNameGenerator::new("iceberg".to_string(), None, DataFileFormat::Parquet);

        let iceberg_schema = table.metadata().current_schema().clone();

        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(Default::default()))
            .build();

        let parquet_builder = ParquetWriterBuilder::new(props, iceberg_schema);

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

        // Commit via fast_append
        let tx = Transaction::new(table);
        let action = tx.fast_append().add_data_files(data_files);
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

/// Create the SQL catalog from config.
async fn create_catalog(config: &IcebergConfig) -> Result<Arc<dyn Catalog>> {
    use iceberg::CatalogBuilder;
    use iceberg_catalog_sql::SqlCatalogBuilder;

    let catalog = SqlCatalogBuilder::default()
        .uri(config.catalog_uri.clone())
        .warehouse_location(config.warehouse.clone())
        .load("flux", HashMap::new())
        .await
        .map_err(|e| IcebergError::Iceberg(format!("create catalog: {e}")))?;

    Ok(Arc::new(catalog))
}
