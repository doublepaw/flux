// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::{WriterProperties, WriterVersion};

use crate::buffer::error::Result;

#[derive(Debug, Clone)]
pub struct ParquetWriterConfig {
    pub output_dir: PathBuf,
    pub compression: Compression,
    pub row_group_size: usize,
    pub data_page_size: usize,
    pub writer_version: WriterVersion,
    pub enable_dictionary: bool,
}

impl Default for ParquetWriterConfig {
    fn default() -> Self {
        Self {
            output_dir: PathBuf::from("./output"),
            compression: Compression::ZSTD(Default::default()),
            row_group_size: 1024 * 1024,
            data_page_size: 1024 * 1024,
            writer_version: WriterVersion::PARQUET_2_0,
            enable_dictionary: true,
        }
    }
}

impl ParquetWriterConfig {
    pub fn with_output_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.output_dir = dir.into();
        self
    }

    pub fn with_row_group_size(mut self, size: usize) -> Self {
        self.row_group_size = size;
        self
    }
}

pub struct ParquetFileWriter {
    config: ParquetWriterConfig,
}

impl ParquetFileWriter {
    pub fn new(config: ParquetWriterConfig) -> Result<Self> {
        std::fs::create_dir_all(&config.output_dir)?;
        Ok(Self { config })
    }

    pub fn write_batch(&self, batch: &RecordBatch, filename: &str) -> Result<PathBuf> {
        let path = self.config.output_dir.join(filename);
        let file = File::create(&path)?;
        let props = self.build_writer_properties();

        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
        writer.write(batch)?;
        writer.close()?;
        Ok(path)
    }

    fn build_writer_properties(&self) -> WriterProperties {
        let mut builder = WriterProperties::builder()
            .set_writer_version(self.config.writer_version)
            .set_compression(self.config.compression)
            .set_max_row_group_size(self.config.row_group_size)
            .set_data_page_size_limit(self.config.data_page_size);

        if self.config.enable_dictionary {
            builder = builder.set_dictionary_enabled(true);
        }
        builder.build()
    }
}

pub struct StreamingParquetWriter {
    writer: ArrowWriter<File>,
    path: PathBuf,
    rows_written: usize,
}

impl StreamingParquetWriter {
    pub fn new(
        path: impl Into<PathBuf>,
        schema: Arc<arrow::datatypes::Schema>,
        config: &ParquetWriterConfig,
    ) -> Result<Self> {
        let path = path.into();
        let file = File::create(&path)?;

        let props = WriterProperties::builder()
            .set_writer_version(config.writer_version)
            .set_compression(config.compression)
            .set_max_row_group_size(config.row_group_size)
            .set_data_page_size_limit(config.data_page_size)
            .set_dictionary_enabled(config.enable_dictionary)
            .build();

        let writer = ArrowWriter::try_new(file, schema, Some(props))?;
        Ok(Self {
            writer,
            path,
            rows_written: 0,
        })
    }

    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.rows_written += batch.num_rows();
        self.writer.write(batch)?;
        Ok(())
    }

    pub fn close(self) -> Result<PathBuf> {
        self.writer.close()?;
        Ok(self.path)
    }

    pub fn rows_written(&self) -> usize {
        self.rows_written
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use tempfile::tempdir;

    fn create_test_batch(num_rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("score", DataType::Float64, false),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from((0..num_rows as i64).collect::<Vec<_>>())),
                Arc::new(StringArray::from(
                    (0..num_rows)
                        .map(|i| format!("user_{}", i))
                        .collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(
                    (0..num_rows).map(|i| i as f64 * 1.5).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_write_and_read_batch() {
        let temp_dir = tempdir().unwrap();
        let config = ParquetWriterConfig::default().with_output_dir(temp_dir.path());
        let writer = ParquetFileWriter::new(config).unwrap();

        let batch = create_test_batch(100);
        let path = writer.write_batch(&batch, "test.parquet").unwrap();

        let file = std::fs::File::open(&path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let total_rows: usize = reader.map(|r| r.unwrap().num_rows()).sum();
        assert_eq!(total_rows, 100);
    }

    #[test]
    fn test_streaming_writer() {
        let temp_dir = tempdir().unwrap();
        let config = ParquetWriterConfig::default().with_output_dir(temp_dir.path());
        let path = temp_dir.path().join("streaming.parquet");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("score", DataType::Float64, false),
        ]));

        let mut writer = StreamingParquetWriter::new(&path, schema, &config).unwrap();
        for _ in 0..10 {
            writer.write_batch(&create_test_batch(100)).unwrap();
        }
        assert_eq!(writer.rows_written(), 1000);

        let final_path = writer.close().unwrap();
        let file = std::fs::File::open(&final_path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let total_rows: usize = reader.map(|r| r.unwrap().num_rows()).sum();
        assert_eq!(total_rows, 1000);
    }

    #[test]
    fn test_compression_codecs() {
        let temp_dir = tempdir().unwrap();
        for (i, compression) in [
            Compression::UNCOMPRESSED,
            Compression::SNAPPY,
            Compression::ZSTD(Default::default()),
        ]
        .into_iter()
        .enumerate()
        {
            let mut config = ParquetWriterConfig::default().with_output_dir(temp_dir.path());
            config.compression = compression;
            let writer = ParquetFileWriter::new(config).unwrap();
            let path = writer
                .write_batch(&create_test_batch(50), &format!("c{}.parquet", i))
                .unwrap();
            assert!(path.exists());
        }
    }
}
