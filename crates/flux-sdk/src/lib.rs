// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Flux SDK for producing and consuming messages.
//!
//! # Writer Example
//!
//! ```ignore
//! let writer = Writer::connect("ws://localhost:9000").await?;
//! writer.append(topic_id, schema_id, records).await?;
//! ```
//!
//! # Reader Example
//!
//! ```ignore
//! let config = ReaderConfig {
//!     url: "ws://localhost:9000".to_string(),
//!     group_id: "group-1".to_string(),
//!     reader_id: "reader-1".to_string(),
//!     topic_id,
//!     ..Default::default()
//! };
//! let reader = Reader::join(config).await?;
//! let batch = reader.poll().await?;
//! reader.commit(&batch).await?;
//! ```

pub mod error;
pub mod raw_reader;
pub mod reader;
pub mod writer;

pub use error::SdkError;
pub use raw_reader::{RawReadResult, RawReader, RawReaderConfig, RawSegmentResult};
pub use reader::{PollBatch, Reader, ReaderConfig};
pub use writer::Writer;
