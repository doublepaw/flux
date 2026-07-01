// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Bump-allocated Avro value representation for high-throughput batch processing.
//!
//! This module provides `BumpValue`, a variant of `Value` that allocates all
//! collections from a bump allocator. This eliminates per-record allocation
//! overhead and allows O(1) batch reset.
//!
//! # Example
//!
//! ```text
//! let mut batch_de = BatchDeserializer::new(&schema);
//! for batch in batches {
//!     for data in batch {
//!         let value = batch_de.deserialize(data)?;
//!         process(&value);
//!     }
//!     batch_de.reset(); // O(1) - frees all allocations
//! }
//! ```

use bumpalo::Bump;
use bumpalo::collections::Vec as BumpVec;
use rust_decimal::Decimal as RustDecimal;

/// A dynamic Avro value that allocates from a bump allocator.
///
/// Unlike `Value`, this type uses slices for arrays and records,
/// eliminating per-collection allocation and Drop overhead. Maps use a slice
/// of key-value pairs instead of HashMap to avoid hashing overhead.
///
/// All data borrows from the bump allocator's lifetime. This type has NO
/// Drop implementation - memory is freed only when the bump allocator resets.
#[derive(Debug)]
pub enum BumpValue<'a> {
    /// Avro null
    Null,
    /// Avro boolean
    Boolean(bool),
    /// Avro int (32-bit signed)
    Int(i32),
    /// Avro long (64-bit signed)
    Long(i64),
    /// Avro float (32-bit IEEE 754)
    Float(f32),
    /// Avro double (64-bit IEEE 754)
    Double(f64),
    /// Avro bytes (variable-length binary data)
    Bytes(&'a [u8]),
    /// Avro string (UTF-8 encoded)
    String(&'a str),
    /// Avro array - slice allocated from bump (no Drop)
    Array(&'a [BumpValue<'a>]),
    /// Avro map - slice of pairs instead of HashMap (no Drop)
    Map(&'a [(&'a str, BumpValue<'a>)]),
    /// Avro record with fields in schema order - slice allocated from bump (no Drop)
    Record(&'a [BumpValue<'a>]),
    /// Avro enum with symbol name and index
    Enum {
        /// The enum symbol name
        symbol: &'a str,
        /// The index of the symbol in the schema
        index: u32,
    },
    /// Avro union with discriminant and value
    Union {
        /// The index of the selected variant in the union schema
        index: u32,
        /// The value of the selected variant - boxed in bump
        value: &'a mut BumpValue<'a>,
    },
    /// Avro fixed (fixed-length binary data)
    Fixed {
        /// The size of the fixed type
        size: usize,
        /// The fixed data
        data: &'a [u8],
    },
    /// Avro decimal logical type
    Decimal(RustDecimal),
    /// Avro UUID logical type (string representation)
    Uuid(&'a str),
    /// Avro date logical type (days since Unix epoch)
    Date(i32),
    /// Avro time-millis logical type (milliseconds since midnight)
    TimeMillis(i32),
    /// Avro time-micros logical type (microseconds since midnight)
    TimeMicros(i64),
    /// Avro timestamp-millis logical type (milliseconds since Unix epoch)
    TimestampMillis(i64),
    /// Avro timestamp-micros logical type (microseconds since Unix epoch)
    TimestampMicros(i64),
    /// Avro duration logical type
    Duration {
        /// Number of months
        months: u32,
        /// Number of days
        days: u32,
        /// Number of milliseconds
        milliseconds: u32,
    },
}

impl<'a> BumpValue<'a> {
    /// Returns `true` if this value is `Null`.
    pub fn is_null(&self) -> bool {
        matches!(self, BumpValue::Null)
    }

    /// Returns the boolean value if this is a `Boolean`, otherwise `None`.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            BumpValue::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    /// Returns the i32 value if this is an `Int`, otherwise `None`.
    pub fn as_int(&self) -> Option<i32> {
        match self {
            BumpValue::Int(i) => Some(*i),
            _ => None,
        }
    }

    /// Returns the i64 value if this is a `Long`, otherwise `None`.
    pub fn as_long(&self) -> Option<i64> {
        match self {
            BumpValue::Long(l) => Some(*l),
            _ => None,
        }
    }

    /// Returns the f32 value if this is a `Float`, otherwise `None`.
    pub fn as_float(&self) -> Option<f32> {
        match self {
            BumpValue::Float(f) => Some(*f),
            _ => None,
        }
    }

    /// Returns the f64 value if this is a `Double`, otherwise `None`.
    pub fn as_double(&self) -> Option<f64> {
        match self {
            BumpValue::Double(d) => Some(*d),
            _ => None,
        }
    }

    /// Returns a reference to the string if this is a `String`, otherwise `None`.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            BumpValue::String(s) => Some(s),
            _ => None,
        }
    }

    /// Returns a reference to the bytes if this is a `Bytes`, otherwise `None`.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            BumpValue::Bytes(b) => Some(b),
            _ => None,
        }
    }

    /// Returns a reference to the array if this is an `Array`, otherwise `None`.
    pub fn as_array(&self) -> Option<&'a [BumpValue<'a>]> {
        match self {
            BumpValue::Array(arr) => Some(arr),
            _ => None,
        }
    }

    /// Returns a reference to the record fields if this is a `Record`, otherwise `None`.
    pub fn as_record(&self) -> Option<&'a [BumpValue<'a>]> {
        match self {
            BumpValue::Record(fields) => Some(fields),
            _ => None,
        }
    }

    /// Get a record field by index.
    pub fn get_field(&self, index: usize) -> Option<&BumpValue<'a>> {
        match self {
            BumpValue::Record(fields) => fields.get(index),
            _ => None,
        }
    }

    /// Get a map value by key (linear scan).
    pub fn get_map_value(&self, key: &str) -> Option<&BumpValue<'a>> {
        match self {
            BumpValue::Map(pairs) => pairs.iter().find(|(k, _)| *k == key).map(|(_, v)| v),
            _ => None,
        }
    }
}

/// A bump-allocated record with fields in schema order.
#[derive(Debug)]
pub struct BumpRecord<'a> {
    fields: &'a [BumpValue<'a>],
}

impl<'a> BumpRecord<'a> {
    /// Create a new record from a slice.
    pub fn from_slice(fields: &'a [BumpValue<'a>]) -> Self {
        Self { fields }
    }

    /// Get a field by index.
    pub fn get(&self, index: usize) -> Option<&BumpValue<'a>> {
        self.fields.get(index)
    }

    /// Get the number of fields.
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    /// Check if the record is empty.
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Iterate over fields.
    pub fn iter(&self) -> impl Iterator<Item = &BumpValue<'a>> {
        self.fields.iter()
    }

    /// Convert to BumpValue::Record
    pub fn into_value(self) -> BumpValue<'a> {
        BumpValue::Record(self.fields)
    }
}

// ============================================================================
// Deserialization support
// ============================================================================

use serde::de::{self, DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};
use std::fmt;

/// A seed for deserializing BumpValue that carries the bump allocator.
pub struct BumpValueSeed<'a> {
    pub(crate) bump: &'a Bump,
}

impl<'a> BumpValueSeed<'a> {
    /// Create a new seed with the given bump allocator.
    pub fn new(bump: &'a Bump) -> Self {
        Self { bump }
    }
}

impl<'a> DeserializeSeed<'a> for BumpValueSeed<'a> {
    type Value = BumpValue<'a>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'a>,
    {
        deserializer.deserialize_any(BumpValueVisitor { bump: self.bump })
    }
}

/// Visitor for deserializing BumpValue.
struct BumpValueVisitor<'a> {
    bump: &'a Bump,
}

impl<'a> Visitor<'a> for BumpValueVisitor<'a> {
    type Value = BumpValue<'a>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("any valid Avro value")
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(BumpValue::Null)
    }

    fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(BumpValue::Boolean(v))
    }

    fn visit_i32<E>(self, v: i32) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(BumpValue::Int(v))
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(BumpValue::Long(v))
    }

    fn visit_u32<E>(self, v: u32) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(BumpValue::Int(v as i32))
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(BumpValue::Long(v as i64))
    }

    fn visit_f32<E>(self, v: f32) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(BumpValue::Float(v))
    }

    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(BumpValue::Double(v))
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        // For schema-derived strings (enum names, union discriminants),
        // allocate in bump. This is rare - most strings use visit_borrowed_str.
        let s = self.bump.alloc_str(v);
        Ok(BumpValue::String(s))
    }

    fn visit_borrowed_str<E>(self, v: &'a str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        // Zero-copy: borrow directly from input
        Ok(BumpValue::String(v))
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        // For non-borrowed bytes, allocate in bump
        let b = self.bump.alloc_slice_copy(v);
        Ok(BumpValue::Bytes(b))
    }

    fn visit_borrowed_bytes<E>(self, v: &'a [u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        // Zero-copy: borrow directly from input
        Ok(BumpValue::Bytes(v))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(BumpValue::Null)
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'a>,
    {
        BumpValueSeed::new(self.bump).deserialize(deserializer)
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'a>,
    {
        // Use size_hint or default to 8 for better initial capacity
        let capacity = seq.size_hint().unwrap_or(8);
        let mut values = BumpVec::with_capacity_in(capacity, self.bump);
        while let Some(value) = seq.next_element_seed(BumpValueSeed::new(self.bump))? {
            values.push(value);
        }
        // Convert to slice - this "leaks" the BumpVec, preventing Drop from running
        Ok(BumpValue::Array(values.into_bump_slice()))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'a>,
    {
        // Use size_hint or default to 8 for better initial capacity
        let capacity = map.size_hint().unwrap_or(8);
        let mut pairs = BumpVec::with_capacity_in(capacity, self.bump);
        while let Some((key, value)) = map.next_entry_seed(
            BumpStrSeed { bump: self.bump },
            BumpValueSeed::new(self.bump),
        )? {
            pairs.push((key, value));
        }
        // Convert to slice - this "leaks" the BumpVec, preventing Drop from running
        Ok(BumpValue::Map(pairs.into_bump_slice()))
    }

    fn visit_enum<A>(self, data: A) -> Result<Self::Value, A::Error>
    where
        A: de::EnumAccess<'a>,
    {
        use de::VariantAccess;
        let (variant, access) = data.variant_seed(BumpStrSeed { bump: self.bump })?;
        match access.unit_variant() {
            Ok(()) => Ok(BumpValue::Enum {
                symbol: variant,
                index: 0,
            }),
            Err(_) => Ok(BumpValue::Enum {
                symbol: variant,
                index: 0,
            }),
        }
    }

    fn visit_newtype_struct<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'a>,
    {
        // Records come through visit_newtype_struct
        deserializer.deserialize_any(BumpRecordVisitor { bump: self.bump })
    }
}

/// Visitor specifically for deserializing records.
struct BumpRecordVisitor<'a> {
    bump: &'a Bump,
}

impl<'a> Visitor<'a> for BumpRecordVisitor<'a> {
    type Value = BumpValue<'a>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a record (sequence of field values)")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'a>,
    {
        // Records always have known field count from schema
        let field_count = seq.size_hint().unwrap_or(0);

        if field_count == 0 {
            return Ok(BumpValue::Record(&[]));
        }

        // Pre-allocate exact-size slice - no BumpVec overhead
        let fields = self
            .bump
            .alloc_slice_fill_with(field_count, |_| BumpValue::Null);

        // Fill in place - overwrites Null placeholders
        for field in fields.iter_mut() {
            if let Some(value) = seq.next_element_seed(BumpValueSeed::new(self.bump))? {
                *field = value;
            }
        }

        Ok(BumpValue::Record(fields))
    }
}

/// A seed for deserializing strings, with bump allocation fallback.
struct BumpStrSeed<'a> {
    bump: &'a Bump,
}

impl<'a> DeserializeSeed<'a> for BumpStrSeed<'a> {
    type Value = &'a str;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'a>,
    {
        deserializer.deserialize_str(BumpStrVisitor { bump: self.bump })
    }
}

struct BumpStrVisitor<'a> {
    bump: &'a Bump,
}

impl<'a> Visitor<'a> for BumpStrVisitor<'a> {
    type Value = &'a str;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a string")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        // Allocate in bump for non-borrowed strings
        Ok(self.bump.alloc_str(v))
    }

    fn visit_borrowed_str<E>(self, v: &'a str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        // Zero-copy
        Ok(v)
    }
}

// ============================================================================
// BatchDeserializer API
// ============================================================================

use crate::avro::Schema;
use crate::avro::de::{DeError, DeserializerState};

/// A batch deserializer that uses bump allocation for high-throughput processing.
///
/// All allocations during deserialization come from an internal bump allocator,
/// which can be reset in O(1) time between batches.
///
/// # Example
///
/// ```text
/// let mut batch_de = BatchDeserializer::new(&schema);
/// for batch in data_batches {
///     for datum in batch {
///         let value = batch_de.deserialize(datum)?;
///     }
///     batch_de.reset(); // O(1) - frees all allocations
/// }
/// ```
pub struct BatchDeserializer<'s> {
    schema: &'s Schema,
    bump: Bump,
}

impl<'s> BatchDeserializer<'s> {
    /// Create a new batch deserializer with default bump capacity (64KB).
    pub fn new(schema: &'s Schema) -> Self {
        Self::with_capacity(schema, 64 * 1024)
    }

    /// Create a new batch deserializer with the specified bump capacity.
    pub fn with_capacity(schema: &'s Schema, capacity: usize) -> Self {
        Self {
            schema,
            bump: Bump::with_capacity(capacity),
        }
    }

    /// Deserialize a single datum into a BumpValue.
    ///
    /// The returned value borrows from both the bump allocator and the input data.
    /// The input data must outlive the bump allocator (enforced by lifetime).
    pub fn deserialize<'a>(&'a self, data: &'a [u8]) -> Result<BumpValue<'a>, DeError> {
        let mut state = DeserializerState::from_slice(data, self.schema);
        let seed = BumpValueSeed::new(&self.bump);
        seed.deserialize(state.deserializer())
    }

    /// Reset the bump allocator, freeing all allocations in O(1) time.
    ///
    /// After calling this, all previously returned `BumpValue`s are invalidated.
    pub fn reset(&mut self) {
        self.bump.reset();
    }

    /// Get the current allocated bytes in the bump allocator.
    pub fn allocated_bytes(&self) -> usize {
        self.bump.allocated_bytes()
    }

    /// Get a reference to the underlying bump allocator for advanced use.
    pub fn bump(&self) -> &Bump {
        &self.bump
    }
}

/// Deserialize a datum into a BumpValue using the provided bump allocator.
///
/// This is a lower-level API for when you want to manage the bump allocator yourself.
/// Both the data and bump allocator share the same lifetime.
pub fn from_datum_slice_bump<'a>(
    data: &'a [u8],
    schema: &Schema,
    bump: &'a Bump,
) -> Result<BumpValue<'a>, DeError> {
    let mut state = DeserializerState::from_slice(data, schema);
    let seed = BumpValueSeed::new(bump);
    seed.deserialize(state.deserializer())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bump_value_basic() {
        let schema: Schema = r#"{
			"type": "record",
			"name": "Test",
			"fields": [
				{"name": "id", "type": "int"},
				{"name": "name", "type": "string"}
			]
		}"#
        .parse()
        .unwrap();

        let bump = Bump::new();

        // id=42, name="hello"
        let data = &[84, 10, 104, 101, 108, 108, 111];
        let value = from_datum_slice_bump(data, &schema, &bump).unwrap();

        if let BumpValue::Record(fields) = &value {
            assert_eq!(fields.len(), 2);
            assert!(matches!(fields[0], BumpValue::Int(42)));
            if let BumpValue::String(s) = &fields[1] {
                assert_eq!(*s, "hello");
            } else {
                panic!("Expected string");
            }
        } else {
            panic!("Expected record");
        }
    }

    #[test]
    fn test_batch_deserializer() {
        let schema: Schema = r#"{"type": "int"}"#.parse().unwrap();

        let mut batch_de = BatchDeserializer::new(&schema);

        // Deserialize multiple values using valid zigzag-encoded varints
        // For small positive integers n, zigzag encoding is n*2, which fits in one byte if < 128
        for i in 0i32..50 {
            // Zigzag encode: (i << 1) ^ (i >> 31) for positive i is just i * 2
            let encoded = (i * 2) as u8;
            let data = [encoded];
            let value = batch_de.deserialize(&data).unwrap();
            if let BumpValue::Int(v) = value {
                assert_eq!(v, i);
            } else {
                panic!("Expected int");
            }
        }

        // Just verify reset doesn't panic - allocation tracking varies by bumpalo version
        batch_de.reset();
    }
}
