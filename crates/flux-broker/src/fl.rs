// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! FL file format: footer-indexed container for Avro batches.
//!
//! File structure:
//! ```text
//! ┌──────────────────────────────────┐
//! │ RecordBatch 0 data (ZSTD compressed) │
//! ├──────────────────────────────────┤
//! │ RecordBatch 1 data (ZSTD compressed) │
//! ├──────────────────────────────────┤
//! │ ...                              │
//! ├──────────────────────────────────┤
//! │ Footer (batch index)           │
//! │ Footer length (4B, big-endian)   │
//! │ Magic (4B): "FLRN"               │
//! └──────────────────────────────────┘
//! ```

use bytes::{Bytes, BytesMut};
use flux_common::ids::{Offset, SchemaId, TopicId};
use flux_common::types::{Record, RecordBatch};
use flux_wire::varint;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

/// Magic bytes for FL format.
const MAGIC: &[u8; 4] = b"FLRN";

/// ZSTD compression level.

/// Error type for FL operations.
#[derive(Debug, Error)]
pub enum FlError {
    #[error("invalid magic: expected FLRN")]
    InvalidMagic,

    #[error("invalid footer: {0}")]
    InvalidFooter(String),

    #[error("compression error: {0}")]
    Compression(String),

    #[error("decompression error: {0}")]
    Decompression(String),

    #[error("crc32 mismatch: expected {expected}, got {actual}")]
    CrcMismatch { expected: u32, actual: u32 },

    #[error("decode error: {0}")]
    Decode(String),

    #[error("buffer too small: need {needed}, have {available}")]
    BufferTooSmall { needed: usize, available: usize },
}

/// Compression codec.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Codec {
    Zstd = 0,
}

/// Metadata for a single batch within a FL file.
#[derive(Debug, Clone)]
pub struct SegmentMeta {
    pub topic_id: TopicId,
    pub schema_id: SchemaId,
    pub start_offset: Offset,
    pub end_offset: Offset,
    pub record_count: u32,
    pub byte_offset: u64,
    pub byte_length: u64,
    pub ingest_time: u64, // micros since epoch
    pub compression: Codec,
    pub crc32: u32,
}

/// FL file writer.
pub struct FlWriter {
    buffer: BytesMut,
    segment_metas: Vec<SegmentMeta>,
}

impl Default for FlWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl FlWriter {
    /// Create a new FL writer.
    pub fn new() -> Self {
        Self {
            buffer: BytesMut::new(),
            segment_metas: Vec::new(),
        }
    }

    /// Add a batch to the file.
    ///
    /// Returns the metadata for the added batch (offsets are provisional zeros).
    pub fn add_segment(&mut self, batch: &RecordBatch) -> Result<SegmentMeta, FlError> {
        // Encode + compress via the shared segment codec (flux_wire::segment
        // is the reference implementation mirrored by the Java/Python SDKs).
        let (compressed, crc) = flux_wire::segment::encode_segment(&batch.records)
            .map_err(|e| FlError::Compression(e.to_string()))?;

        // 4. Record byte offset and write
        let byte_offset = self.buffer.len() as u64;
        self.buffer.extend_from_slice(&compressed);
        let byte_length = compressed.len() as u64;

        // 5. Get current time in micros
        let ingest_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        let meta = SegmentMeta {
            topic_id: batch.topic_id,
            schema_id: batch.schema_id,
            start_offset: Offset(0), // Provisional, filled by DB commit
            end_offset: Offset(0),   // Provisional, filled by DB commit
            record_count: batch.records.len() as u32,
            byte_offset,
            byte_length,
            ingest_time,
            compression: Codec::Zstd,
            crc32: crc,
        };

        self.segment_metas.push(meta.clone());
        Ok(meta)
    }

    /// Finish writing and return the complete file contents.
    pub fn finish(mut self) -> Bytes {
        // 1. Encode batch metadata as footer
        let footer_bytes = encode_footer(&self.segment_metas);
        let footer_start = self.buffer.len();
        self.buffer.extend_from_slice(&footer_bytes);
        let footer_length = self.buffer.len() - footer_start;

        // 2. Write footer length (4 bytes, big-endian)
        self.buffer
            .extend_from_slice(&(footer_length as u32).to_be_bytes());

        // 3. Write magic
        self.buffer.extend_from_slice(MAGIC);

        self.buffer.freeze()
    }

    /// Get the current batch metadata.
    pub fn segment_metas(&self) -> &[SegmentMeta] {
        &self.segment_metas
    }
}

/// FL file reader.
pub struct FlReader;

impl FlReader {
    /// Read and parse the footer from a FL file.
    pub fn read_footer(data: &[u8]) -> Result<Vec<SegmentMeta>, FlError> {
        if data.len() < 8 {
            return Err(FlError::InvalidFooter("file too small".into()));
        }

        // 1. Check magic
        let magic = &data[data.len() - 4..];
        if magic != MAGIC {
            return Err(FlError::InvalidMagic);
        }

        // 2. Read footer length
        let footer_len_bytes = &data[data.len() - 8..data.len() - 4];
        let footer_length = u32::from_be_bytes(footer_len_bytes.try_into().unwrap()) as usize;

        // 3. Read footer data
        let footer_end = data.len() - 8;
        if footer_length > footer_end {
            return Err(FlError::InvalidFooter(
                "footer length exceeds file size".into(),
            ));
        }
        let footer_start = footer_end - footer_length;
        let footer_bytes = &data[footer_start..footer_end];

        // 4. Decode footer
        decode_footer(footer_bytes)
    }

    /// Read a batch's records given its metadata.
    ///
    /// The `data` parameter should contain the raw file bytes.
    pub fn read_segment(
        data: &[u8],
        meta: &SegmentMeta,
        verify_crc: bool,
    ) -> Result<Vec<Record>, FlError> {
        let start = meta.byte_offset as usize;
        let end = start + meta.byte_length as usize;

        if end > data.len() {
            return Err(FlError::BufferTooSmall {
                needed: end,
                available: data.len(),
            });
        }

        let compressed = &data[start..end];

        // Shared segment codec (reference implementation for all SDKs).
        let expected_crc = verify_crc.then_some(meta.crc32);
        flux_wire::segment::decode_segment(compressed, expected_crc).map_err(|e| match e {
            flux_wire::segment::SegmentError::CrcMismatch { expected, actual } => {
                FlError::CrcMismatch { expected, actual }
            }
            flux_wire::segment::SegmentError::Decompression(msg) => FlError::Decompression(msg),
            other => FlError::Decode(other.to_string()),
        })
    }
}

/// Encode batch metadata as footer bytes.
fn encode_footer(metas: &[SegmentMeta]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(metas.len() * 64);
    let mut varint_buf = [0u8; 10];

    // Write count
    let len = varint::encode_i64(metas.len() as i64, &mut varint_buf);
    buf.extend_from_slice(&varint_buf[..len]);

    for meta in metas {
        // topic_id
        let len = varint::encode_i64(meta.topic_id.0 as i64, &mut varint_buf);
        buf.extend_from_slice(&varint_buf[..len]);

        // schema_id
        let len = varint::encode_i64(meta.schema_id.0 as i64, &mut varint_buf);
        buf.extend_from_slice(&varint_buf[..len]);

        // start_offset
        let len = varint::encode_u64(meta.start_offset.0, &mut varint_buf);
        buf.extend_from_slice(&varint_buf[..len]);

        // end_offset
        let len = varint::encode_u64(meta.end_offset.0, &mut varint_buf);
        buf.extend_from_slice(&varint_buf[..len]);

        // record_count
        let len = varint::encode_i64(meta.record_count as i64, &mut varint_buf);
        buf.extend_from_slice(&varint_buf[..len]);

        // byte_offset
        let len = varint::encode_u64(meta.byte_offset, &mut varint_buf);
        buf.extend_from_slice(&varint_buf[..len]);

        // byte_length
        let len = varint::encode_u64(meta.byte_length, &mut varint_buf);
        buf.extend_from_slice(&varint_buf[..len]);

        // ingest_time
        let len = varint::encode_u64(meta.ingest_time, &mut varint_buf);
        buf.extend_from_slice(&varint_buf[..len]);

        // compression (single byte)
        buf.push(meta.compression as u8);

        // crc32 (4 bytes, big-endian)
        buf.extend_from_slice(&meta.crc32.to_be_bytes());
    }

    buf
}

/// Decode footer bytes into batch metadata.
fn decode_footer(data: &[u8]) -> Result<Vec<SegmentMeta>, FlError> {
    let mut offset = 0;

    // Read count
    let (count, len) =
        varint::decode_i64(data).map_err(|e| FlError::InvalidFooter(format!("count: {}", e)))?;
    offset += len;

    let mut metas = Vec::with_capacity(count as usize);

    for _ in 0..count {
        // topic_id
        let (topic_id, len) = varint::decode_i64(&data[offset..])
            .map_err(|e| FlError::InvalidFooter(format!("topic_id: {}", e)))?;
        offset += len;

        // schema_id
        let (schema_id, len) = varint::decode_i64(&data[offset..])
            .map_err(|e| FlError::InvalidFooter(format!("schema_id: {}", e)))?;
        offset += len;

        // start_offset
        let (start_offset, len) = varint::decode_u64(&data[offset..])
            .map_err(|e| FlError::InvalidFooter(format!("start_offset: {}", e)))?;
        offset += len;

        // end_offset
        let (end_offset, len) = varint::decode_u64(&data[offset..])
            .map_err(|e| FlError::InvalidFooter(format!("end_offset: {}", e)))?;
        offset += len;

        // record_count
        let (record_count, len) = varint::decode_i64(&data[offset..])
            .map_err(|e| FlError::InvalidFooter(format!("record_count: {}", e)))?;
        offset += len;

        // byte_offset
        let (byte_offset, len) = varint::decode_u64(&data[offset..])
            .map_err(|e| FlError::InvalidFooter(format!("byte_offset: {}", e)))?;
        offset += len;

        // byte_length
        let (byte_length, len) = varint::decode_u64(&data[offset..])
            .map_err(|e| FlError::InvalidFooter(format!("byte_length: {}", e)))?;
        offset += len;

        // ingest_time
        let (ingest_time, len) = varint::decode_u64(&data[offset..])
            .map_err(|e| FlError::InvalidFooter(format!("ingest_time: {}", e)))?;
        offset += len;

        // compression
        if offset >= data.len() {
            return Err(FlError::InvalidFooter("unexpected end of footer".into()));
        }
        let compression = match data[offset] {
            0 => Codec::Zstd,
            _ => return Err(FlError::InvalidFooter("unknown compression codec".into())),
        };
        offset += 1;

        // crc32
        if offset + 4 > data.len() {
            return Err(FlError::InvalidFooter("crc32 truncated".into()));
        }
        let crc32 = u32::from_be_bytes(data[offset..offset + 4].try_into().unwrap());
        offset += 4;

        metas.push(SegmentMeta {
            topic_id: TopicId(topic_id as u32),
            schema_id: SchemaId(schema_id as u32),
            start_offset: Offset(start_offset),
            end_offset: Offset(end_offset),
            record_count: record_count as u32,
            byte_offset,
            byte_length,
            ingest_time,
            compression,
            crc32,
        });
    }

    Ok(metas)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_segment() -> RecordBatch {
        RecordBatch {
            topic_id: TopicId(1),
            schema_id: SchemaId(100),
            records: vec![
                Record {
                    key: Some(Bytes::from_static(b"key1")),
                    value: Bytes::from_static(b"value1"),
                },
                Record {
                    key: None,
                    value: Bytes::from_static(b"value2"),
                },
                Record {
                    key: Some(Bytes::from_static(b"key3")),
                    value: Bytes::from_static(b"value3value3value3"),
                },
            ],
        }
    }

    #[test]
    fn test_fl_write_read_roundtrip() {
        let batch = sample_segment();

        // Write
        let mut writer = FlWriter::new();
        let meta = writer.add_segment(&batch).unwrap();
        let file_bytes = writer.finish();

        // Verify magic
        assert_eq!(&file_bytes[file_bytes.len() - 4..], MAGIC);

        // Read footer
        let metas = FlReader::read_footer(&file_bytes).unwrap();
        assert_eq!(metas.len(), 1);
        assert_eq!(metas[0].topic_id.0, 1);
        assert_eq!(metas[0].record_count, 3);
        assert_eq!(metas[0].byte_offset, meta.byte_offset);
        assert_eq!(metas[0].byte_length, meta.byte_length);

        // Read batch
        let records = FlReader::read_segment(&file_bytes, &metas[0], true).unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].key.as_ref().unwrap().as_ref(), b"key1");
        assert_eq!(records[0].value.as_ref(), b"value1");
        assert!(records[1].key.is_none());
        assert_eq!(records[1].value.as_ref(), b"value2");
    }

    #[test]
    fn test_fl_multiple_segments() {
        let segment1 = RecordBatch {
            topic_id: TopicId(1),
            schema_id: SchemaId(100),
            records: vec![Record {
                key: None,
                value: Bytes::from_static(b"segment1"),
            }],
        };

        let segment2 = RecordBatch {
            topic_id: TopicId(2),
            schema_id: SchemaId(200),
            records: vec![
                Record {
                    key: None,
                    value: Bytes::from_static(b"segment2a"),
                },
                Record {
                    key: None,
                    value: Bytes::from_static(b"segment2b"),
                },
            ],
        };

        // Write
        let mut writer = FlWriter::new();
        writer.add_segment(&segment1).unwrap();
        writer.add_segment(&segment2).unwrap();
        let file_bytes = writer.finish();

        // Read footer
        let metas = FlReader::read_footer(&file_bytes).unwrap();
        assert_eq!(metas.len(), 2);

        // First batch
        assert_eq!(metas[0].topic_id.0, 1);
        assert_eq!(metas[0].record_count, 1);

        // Second batch
        assert_eq!(metas[1].topic_id.0, 2);
        assert_eq!(metas[1].record_count, 2);

        // Read both batches
        let records1 = FlReader::read_segment(&file_bytes, &metas[0], true).unwrap();
        assert_eq!(records1.len(), 1);

        let records2 = FlReader::read_segment(&file_bytes, &metas[1], true).unwrap();
        assert_eq!(records2.len(), 2);
    }

    #[test]
    fn test_fl_crc_validation() {
        let batch = sample_segment();

        let mut writer = FlWriter::new();
        writer.add_segment(&batch).unwrap();
        let mut file_bytes = writer.finish().to_vec();

        // Read footer first to get metadata
        let metas = FlReader::read_footer(&file_bytes).unwrap();

        // Corrupt the data
        file_bytes[0] ^= 0xFF;

        // CRC check should fail
        let result = FlReader::read_segment(&file_bytes, &metas[0], true);
        assert!(matches!(result, Err(FlError::CrcMismatch { .. })));
    }

    #[test]
    fn test_fl_invalid_magic() {
        let mut data = vec![0u8; 16];
        data[12..16].copy_from_slice(b"NOPE");

        let result = FlReader::read_footer(&data);
        assert!(matches!(result, Err(FlError::InvalidMagic)));
    }

    #[test]
    fn test_fl_compression_ratio() {
        // Create a batch with repetitive data (should compress well)
        let batch = RecordBatch {
            topic_id: TopicId(1),
            schema_id: SchemaId(100),
            records: (0..100)
                .map(|i| Record {
                    key: Some(Bytes::from(format!("key{}", i))),
                    value: Bytes::from("this is a value that repeats many times".repeat(10)),
                })
                .collect(),
        };

        let mut writer = FlWriter::new();
        let meta = writer.add_segment(&batch).unwrap();

        // Compressed size should be significantly smaller than uncompressed
        let estimated_uncompressed = 100 * (10 + 400); // rough estimate
        assert!(
            meta.byte_length < estimated_uncompressed as u64 / 2,
            "compression ratio should be at least 2x, got {} vs {}",
            meta.byte_length,
            estimated_uncompressed
        );
    }

    #[test]
    fn test_fl_empty_segment() {
        let batch = RecordBatch {
            topic_id: TopicId(1),
            schema_id: SchemaId(100),
            records: vec![],
        };

        let mut writer = FlWriter::new();
        writer.add_segment(&batch).unwrap();
        let file_bytes = writer.finish();

        let metas = FlReader::read_footer(&file_bytes).unwrap();
        assert_eq!(metas.len(), 1);
        assert_eq!(metas[0].record_count, 0);

        let records = FlReader::read_segment(&file_bytes, &metas[0], true).unwrap();
        assert!(records.is_empty());
    }

    // ============ Edge Case Tests ============

    #[test]
    fn test_fl_many_segments() {
        let mut writer = FlWriter::new();

        // Write 50 batches
        for i in 0..50 {
            let batch = RecordBatch {
                topic_id: TopicId(i),
                schema_id: SchemaId(100 + i),
                records: vec![Record {
                    key: Some(Bytes::from(format!("key-{}", i))),
                    value: Bytes::from(format!("value-{}", i)),
                }],
            };
            writer.add_segment(&batch).unwrap();
        }

        let file_bytes = writer.finish();
        let metas = FlReader::read_footer(&file_bytes).unwrap();

        assert_eq!(metas.len(), 50);

        // Verify each batch
        for (i, meta) in metas.iter().enumerate() {
            assert_eq!(meta.topic_id.0, i as u32);
            assert_eq!(meta.record_count, 1);

            let records = FlReader::read_segment(&file_bytes, meta, true).unwrap();
            assert_eq!(records.len(), 1);
        }
    }

    #[test]
    fn test_fl_large_record_values() {
        // Test with 1MB values
        let large_value = vec![0xABu8; 1024 * 1024];

        let batch = RecordBatch {
            topic_id: TopicId(1),
            schema_id: SchemaId(100),
            records: vec![Record {
                key: Some(Bytes::from_static(b"large")),
                value: Bytes::from(large_value.clone()),
            }],
        };

        let mut writer = FlWriter::new();
        writer.add_segment(&batch).unwrap();
        let file_bytes = writer.finish();

        let metas = FlReader::read_footer(&file_bytes).unwrap();
        let records = FlReader::read_segment(&file_bytes, &metas[0], true).unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].value.len(), 1024 * 1024);
        assert_eq!(records[0].value.as_ref(), large_value.as_slice());
    }

    #[test]
    fn test_fl_binary_keys_and_values() {
        // Non-UTF8 binary data
        let binary_key = vec![0x00, 0xFF, 0x80, 0x7F, 0x01, 0xFE];
        let binary_value = (0..=255u8).collect::<Vec<u8>>();

        let batch = RecordBatch {
            topic_id: TopicId(1),
            schema_id: SchemaId(100),
            records: vec![Record {
                key: Some(Bytes::from(binary_key.clone())),
                value: Bytes::from(binary_value.clone()),
            }],
        };

        let mut writer = FlWriter::new();
        writer.add_segment(&batch).unwrap();
        let file_bytes = writer.finish();

        let metas = FlReader::read_footer(&file_bytes).unwrap();
        let records = FlReader::read_segment(&file_bytes, &metas[0], true).unwrap();

        assert_eq!(
            records[0].key.as_ref().unwrap().as_ref(),
            binary_key.as_slice()
        );
        assert_eq!(records[0].value.as_ref(), binary_value.as_slice());
    }

    #[test]
    fn test_fl_max_id_values() {
        let batch = RecordBatch {
            topic_id: TopicId(u32::MAX),
            schema_id: SchemaId(u32::MAX),
            records: vec![Record {
                key: None,
                value: Bytes::from_static(b"test"),
            }],
        };

        let mut writer = FlWriter::new();
        writer.add_segment(&batch).unwrap();
        let file_bytes = writer.finish();

        let metas = FlReader::read_footer(&file_bytes).unwrap();
        assert_eq!(metas[0].topic_id.0, u32::MAX);
        assert_eq!(metas[0].schema_id.0, u32::MAX);
    }

    #[test]
    fn test_fl_file_too_small() {
        // File smaller than magic + footer length
        let small_file = vec![0u8; 4];
        let result = FlReader::read_footer(&small_file);
        assert!(matches!(result, Err(FlError::InvalidFooter(_))));
    }

    #[test]
    fn test_fl_footer_length_exceeds_file() {
        // Valid magic but footer length too large
        let mut bad_file = vec![0u8; 16];
        // Set footer length to 1000 (way more than file size)
        bad_file[8..12].copy_from_slice(&1000u32.to_be_bytes());
        // Set magic
        bad_file[12..16].copy_from_slice(MAGIC);

        let result = FlReader::read_footer(&bad_file);
        assert!(matches!(result, Err(FlError::InvalidFooter(_))));
    }

    #[test]
    fn test_fl_segment_byte_range_validation() {
        let batch = RecordBatch {
            topic_id: TopicId(1),
            schema_id: SchemaId(100),
            records: vec![Record {
                key: None,
                value: Bytes::from_static(b"test"),
            }],
        };

        let mut writer = FlWriter::new();
        writer.add_segment(&batch).unwrap();
        let file_bytes = writer.finish();

        let metas = FlReader::read_footer(&file_bytes).unwrap();

        // Create a fake meta with invalid byte range
        let bad_meta = SegmentMeta {
            byte_offset: 10000, // Way past end of file
            byte_length: 100,
            ..metas[0].clone()
        };

        let result = FlReader::read_segment(&file_bytes, &bad_meta, false);
        assert!(matches!(result, Err(FlError::BufferTooSmall { .. })));
    }

    #[test]
    fn test_fl_skip_crc_validation() {
        let batch = RecordBatch {
            topic_id: TopicId(1),
            schema_id: SchemaId(100),
            records: vec![Record {
                key: None,
                value: Bytes::from_static(b"test"),
            }],
        };

        let mut writer = FlWriter::new();
        writer.add_segment(&batch).unwrap();
        let mut file_bytes = writer.finish().to_vec();

        let metas = FlReader::read_footer(&file_bytes).unwrap();

        // Corrupt data
        file_bytes[0] ^= 0xFF;

        // With CRC check - should fail
        let result = FlReader::read_segment(&file_bytes, &metas[0], true);
        assert!(matches!(result, Err(FlError::CrcMismatch { .. })));

        // Without CRC check - should succeed (but data is corrupted)
        let result = FlReader::read_segment(&file_bytes, &metas[0], false);
        // This may or may not fail depending on what got corrupted
        // The key point is we're not getting a CRC error
        assert!(!matches!(result, Err(FlError::CrcMismatch { .. })));
    }

    #[test]
    fn test_fl_many_records_per_segment() {
        let batch = RecordBatch {
            topic_id: TopicId(1),
            schema_id: SchemaId(100),
            records: (0..1000)
                .map(|i| Record {
                    key: if i % 2 == 0 {
                        Some(Bytes::from(format!("key-{}", i)))
                    } else {
                        None
                    },
                    value: Bytes::from(format!("value-{}", i)),
                })
                .collect(),
        };

        let mut writer = FlWriter::new();
        writer.add_segment(&batch).unwrap();
        let file_bytes = writer.finish();

        let metas = FlReader::read_footer(&file_bytes).unwrap();
        assert_eq!(metas[0].record_count, 1000);

        let records = FlReader::read_segment(&file_bytes, &metas[0], true).unwrap();
        assert_eq!(records.len(), 1000);

        // Verify a sample of records
        // Even indices have keys, odd indices don't
        assert_eq!(records[0].key.as_ref().unwrap().as_ref(), b"key-0");
        assert!(records[1].key.is_none());
        assert_eq!(records[998].key.as_ref().unwrap().as_ref(), b"key-998");
        assert!(records[999].key.is_none());
    }
}
