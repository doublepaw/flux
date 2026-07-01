// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Core ID types for Flux eventbus

use std::fmt;

macro_rules! newtype_id {
    ($name:ident, $inner:ty) => {
        impl From<$name> for $inner {
            fn from(id: $name) -> $inner {
                id.0
            }
        }
        impl From<$inner> for $name {
            fn from(val: $inner) -> $name {
                $name(val)
            }
        }
        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

/// Unique identifier for a topic
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct TopicId(pub u32);

/// Unique identifier for a schema in the registry
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct SchemaId(pub u32);

/// Offset within a topic (monotonically increasing)
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct Offset(pub u64);

/// Unique identifier for a writer instance
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct WriterId(pub uuid::Uuid);

/// Sequence number for writer deduplication
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct AppendSeq(pub u64);

newtype_id!(TopicId, u32);
newtype_id!(SchemaId, u32);
newtype_id!(Offset, u64);
newtype_id!(WriterId, uuid::Uuid);
newtype_id!(AppendSeq, u64);

// WriterId specific implementations
impl WriterId {
    /// Create a new random writer ID
    pub fn new() -> Self {
        WriterId(uuid::Uuid::new_v4())
    }

    /// Create a writer ID from a UUID
    pub fn from_uuid(uuid: uuid::Uuid) -> Self {
        WriterId(uuid)
    }

    /// Get the underlying UUID
    pub fn as_uuid(&self) -> &uuid::Uuid {
        &self.0
    }
}

impl Default for WriterId {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_id_into_u32() {
        let id = TopicId(42);
        assert_eq!(u32::from(id), 42);
    }

    #[test]
    fn test_topic_id_from_u32() {
        let id: TopicId = 42u32.into();
        assert_eq!(id.0, 42);
    }

    #[test]
    fn test_offset_ordering() {
        let a = Offset(100);
        let b = Offset(200);
        assert!(a < b);
        assert!(b > a);
        assert_eq!(a, Offset(100));
    }

    #[test]
    fn test_producer_id_display() {
        let id = WriterId(uuid::Uuid::nil());
        assert_eq!(format!("{}", id), "00000000-0000-0000-0000-000000000000");
    }

    #[test]
    fn test_producer_id_new_is_unique() {
        let id1 = WriterId::new();
        let id2 = WriterId::new();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_seq_num_ordering() {
        let a = AppendSeq(1);
        let b = AppendSeq(2);
        assert!(a < b);
    }

    #[test]
    fn test_schema_id_conversions() {
        let id = SchemaId(100);
        let val: u32 = id.into();
        assert_eq!(val, 100);

        let id2: SchemaId = 100u32.into();
        assert_eq!(id, id2);
    }
}
