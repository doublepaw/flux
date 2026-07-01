// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Deserialization implementation for Value.
//!
//! Records are deserialized as sequences (not maps) for performance - field values
//! are stored in schema order without field names.

use super::{GenericRecord, Value};
use serde::de::{self, Deserialize, DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;

/// Visitor for deserializing Value from any Avro data.
struct ValueVisitor<'a> {
    _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a> ValueVisitor<'a> {
    fn new() -> Self {
        Self {
            _marker: std::marker::PhantomData,
        }
    }
}

impl<'de> Visitor<'de> for ValueVisitor<'de> {
    type Value = Value<'de>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("any valid Avro value")
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::Null)
    }

    fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::Boolean(v))
    }

    fn visit_i32<E>(self, v: i32) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::Int(v))
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::Long(v))
    }

    fn visit_u32<E>(self, v: u32) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::Int(v as i32))
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::Long(v as i64))
    }

    fn visit_f32<E>(self, v: f32) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::Float(v))
    }

    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::Double(v))
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::String(Cow::Owned(v.to_owned())))
    }

    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::String(Cow::Borrowed(v)))
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::String(Cow::Owned(v)))
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::Bytes(Cow::Owned(v.to_vec())))
    }

    fn visit_borrowed_bytes<E>(self, v: &'de [u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::Bytes(Cow::Borrowed(v)))
    }

    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::Bytes(Cow::Owned(v)))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Value::Null)
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        Deserialize::deserialize(deserializer)
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        // Sequences are used for both arrays and records
        // We'll return Array here; the deserializer will use RecordSeqAccess for records
        let mut values = match seq.size_hint() {
            Some(size) => Vec::with_capacity(size),
            None => Vec::new(),
        };
        while let Some(value) = seq.next_element()? {
            values.push(value);
        }
        Ok(Value::Array(values))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        // Maps are used for Avro maps (string -> value)
        let mut result = match map.size_hint() {
            Some(size) => HashMap::with_capacity(size),
            None => HashMap::new(),
        };
        while let Some((key, value)) = map.next_entry_seed(CowStrSeed, ValueSeed::new())? {
            result.insert(key, value);
        }
        Ok(Value::Map(result))
    }

    fn visit_enum<A>(self, data: A) -> Result<Self::Value, A::Error>
    where
        A: de::EnumAccess<'de>,
    {
        use de::VariantAccess;
        let (variant, access) = data.variant_seed(CowStrSeed)?;
        // Try to get the value - if it's a unit variant, we get an Enum,
        // otherwise we wrap it in a Union
        match access.unit_variant() {
            Ok(()) => {
                // It was a unit variant - this is an Avro enum
                Ok(Value::Enum {
                    symbol: variant,
                    index: 0, // We don't have the index from serde
                })
            }
            Err(_) => {
                // This shouldn't happen in practice since we already called unit_variant
                // If there was actual data, we would need newtype_variant, but that's
                // handled differently by the deserializer
                Ok(Value::Enum {
                    symbol: variant,
                    index: 0,
                })
            }
        }
    }

    fn visit_newtype_struct<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Records come through visit_newtype_struct to distinguish from arrays
        // The inner deserializer will give us a sequence of field values
        deserializer
            .deserialize_any(RecordValueVisitor::new())
            .map(Value::Record)
    }
}

/// Visitor specifically for deserializing records as sequences of values.
/// This is used by the Avro deserializer to efficiently deserialize records.
pub(crate) struct RecordValueVisitor<'a> {
    _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a> RecordValueVisitor<'a> {
    pub(crate) fn new() -> Self {
        Self {
            _marker: std::marker::PhantomData,
        }
    }
}

impl<'de> Visitor<'de> for RecordValueVisitor<'de> {
    type Value = GenericRecord<'de>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a record (sequence of field values)")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut record = match seq.size_hint() {
            Some(size) => GenericRecord::with_capacity(size),
            None => GenericRecord::with_capacity(0),
        };
        while let Some(value) = seq.next_element()? {
            record.push(value);
        }
        Ok(record)
    }
}

impl<'de> Deserialize<'de> for Value<'de> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(ValueVisitor::new())
    }
}

/// A DeserializeSeed that captures the lifetime properly for borrowed data
pub(crate) struct ValueSeed<'a> {
    _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a> ValueSeed<'a> {
    /// Create a new ValueSeed
    pub(crate) fn new() -> Self {
        Self {
            _marker: std::marker::PhantomData,
        }
    }
}

impl Default for ValueSeed<'_> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'de> DeserializeSeed<'de> for ValueSeed<'de> {
    type Value = Value<'de>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        Value::deserialize(deserializer)
    }
}

/// A seed for deserializing Cow<str> that can borrow from the input
struct CowStrSeed;

impl<'de> DeserializeSeed<'de> for CowStrSeed {
    type Value = Cow<'de, str>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(CowStrVisitor)
    }
}

struct CowStrVisitor;

impl<'de> Visitor<'de> for CowStrVisitor {
    type Value = Cow<'de, str>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a string")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Cow::Owned(v.to_owned()))
    }

    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Cow::Borrowed(v))
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Cow::Owned(v))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Value<'static>>();
    }
}
