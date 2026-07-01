// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Dynamic Avro value representation with zero-copy deserialization support.
//!
//! This module provides a `Value` enum that can represent any Avro value dynamically,
//! similar to `serde_json::Value` but for Avro data. It supports:
//!
//! - **Zero-copy deserialization**: Strings and bytes can borrow from the input slice
//! - **Schema-aware deserialization**: Preserves logical types (Date, Timestamp, UUID, Decimal, Duration)
//! - **O(1) record field lookup**: Use schema's field index for fast access by name
//!
//! # Example
//!
//! ```
//! use flux_core::avro::Schema;
//! use flux_core::avro::value::{Value, from_datum_slice_bump};
//!
//! let schema: Schema = r#"
//! {
//!     "type": "record",
//!     "name": "User",
//!     "fields": [
//!         {"name": "id", "type": "int"},
//!         {"name": "name", "type": "string"}
//!     ]
//! }
//! "#.parse().unwrap();
//!
//! let data = &[4, 8, 74, 111, 104, 110]; // id=2, name="John"
//! let bump = bumpalo::Bump::new();
//! let value = from_datum_slice_bump(data, &schema, &bump).unwrap();
//!
//! use flux_core::avro::value::BumpValue;
//! if let BumpValue::Record(fields) = value {
//!     // Access fields by index (O(1))
//!     assert!(matches!(fields[0], BumpValue::Int(2)));
//!     assert!(matches!(fields[1], BumpValue::String("John")));
//! }
//! ```

pub mod bump;
mod de;
mod record;

pub use bump::{BatchDeserializer, BumpValue, BumpValueSeed, from_datum_slice_bump};
pub use record::GenericRecord;

use rust_decimal::Decimal as RustDecimal;
use std::borrow::Cow;
use std::collections::HashMap;

/// A dynamic Avro value that can represent any Avro type.
///
/// This enum supports zero-copy deserialization through `Cow` types, allowing
/// strings and bytes to borrow directly from the input slice when deserializing
/// from `&[u8]`.
///
/// # Logical Types
///
/// When using schema-aware deserialization via `from_datum_slice`, logical types
/// are preserved:
/// - `Date`, `TimeMillis`, `TimeMicros`, `TimestampMillis`, `TimestampMicros`
/// - `Uuid` (string representation)
/// - `Decimal` (using `rust_decimal::Decimal`)
/// - `Duration` (months, days, milliseconds)
///
/// # Zero-Copy
///
/// `String` and `Bytes` variants use `Cow<'a, str>` and `Cow<'a, [u8]>` respectively,
/// enabling zero-copy deserialization when reading from a slice.
#[derive(Debug, Clone, PartialEq)]
pub enum Value<'a> {
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
    Bytes(Cow<'a, [u8]>),
    /// Avro string (UTF-8 encoded)
    String(Cow<'a, str>),
    /// Avro array
    Array(Vec<Value<'a>>),
    /// Avro map (string keys to values)
    Map(HashMap<Cow<'a, str>, Value<'a>>),
    /// Avro record with named fields
    Record(GenericRecord<'a>),
    /// Avro enum with symbol name and index
    Enum {
        /// The enum symbol name
        symbol: Cow<'a, str>,
        /// The index of the symbol in the schema
        index: u32,
    },
    /// Avro union with discriminant and value
    Union {
        /// The index of the selected variant in the union schema
        index: u32,
        /// The value of the selected variant
        value: Box<Value<'a>>,
    },
    /// Avro fixed (fixed-length binary data)
    Fixed {
        /// The size of the fixed type
        size: usize,
        /// The fixed data
        data: Cow<'a, [u8]>,
    },
    /// Avro decimal logical type
    Decimal(RustDecimal),
    /// Avro UUID logical type (string representation)
    Uuid(Cow<'a, str>),
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

impl<'a> Value<'a> {
    /// Convert this value into an owned version with `'static` lifetime.
    ///
    /// This clones any borrowed data, making the value independent of the
    /// original input slice.
    pub fn into_owned(self) -> Value<'static> {
        match self {
            Value::Null => Value::Null,
            Value::Boolean(b) => Value::Boolean(b),
            Value::Int(i) => Value::Int(i),
            Value::Long(l) => Value::Long(l),
            Value::Float(f) => Value::Float(f),
            Value::Double(d) => Value::Double(d),
            Value::Bytes(b) => Value::Bytes(Cow::Owned(b.into_owned())),
            Value::String(s) => Value::String(Cow::Owned(s.into_owned())),
            Value::Array(arr) => Value::Array(arr.into_iter().map(|v| v.into_owned()).collect()),
            Value::Map(map) => Value::Map(
                map.into_iter()
                    .map(|(k, v)| (Cow::Owned(k.into_owned()), v.into_owned()))
                    .collect(),
            ),
            Value::Record(rec) => Value::Record(rec.into_owned()),
            Value::Enum { symbol, index } => Value::Enum {
                symbol: Cow::Owned(symbol.into_owned()),
                index,
            },
            Value::Union { index, value } => Value::Union {
                index,
                value: Box::new(value.into_owned()),
            },
            Value::Fixed { size, data } => Value::Fixed {
                size,
                data: Cow::Owned(data.into_owned()),
            },
            Value::Decimal(d) => Value::Decimal(d),
            Value::Uuid(u) => Value::Uuid(Cow::Owned(u.into_owned())),
            Value::Date(d) => Value::Date(d),
            Value::TimeMillis(t) => Value::TimeMillis(t),
            Value::TimeMicros(t) => Value::TimeMicros(t),
            Value::TimestampMillis(t) => Value::TimestampMillis(t),
            Value::TimestampMicros(t) => Value::TimestampMicros(t),
            Value::Duration {
                months,
                days,
                milliseconds,
            } => Value::Duration {
                months,
                days,
                milliseconds,
            },
        }
    }

    /// Returns `true` if this value is `Null`.
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Returns the boolean value if this is a `Boolean`, otherwise `None`.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    /// Returns the i32 value if this is an `Int`, otherwise `None`.
    pub fn as_int(&self) -> Option<i32> {
        match self {
            Value::Int(i) => Some(*i),
            _ => None,
        }
    }

    /// Returns the i64 value if this is a `Long`, otherwise `None`.
    pub fn as_long(&self) -> Option<i64> {
        match self {
            Value::Long(l) => Some(*l),
            _ => None,
        }
    }

    /// Returns the f32 value if this is a `Float`, otherwise `None`.
    pub fn as_float(&self) -> Option<f32> {
        match self {
            Value::Float(f) => Some(*f),
            _ => None,
        }
    }

    /// Returns the f64 value if this is a `Double`, otherwise `None`.
    pub fn as_double(&self) -> Option<f64> {
        match self {
            Value::Double(d) => Some(*d),
            _ => None,
        }
    }

    /// Returns a reference to the string if this is a `String`, otherwise `None`.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    /// Returns a reference to the bytes if this is a `Bytes`, otherwise `None`.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Bytes(b) => Some(b),
            _ => None,
        }
    }

    /// Returns a reference to the array if this is an `Array`, otherwise `None`.
    pub fn as_array(&self) -> Option<&[Value<'a>]> {
        match self {
            Value::Array(arr) => Some(arr),
            _ => None,
        }
    }

    /// Returns a reference to the map if this is a `Map`, otherwise `None`.
    pub fn as_map(&self) -> Option<&HashMap<Cow<'a, str>, Value<'a>>> {
        match self {
            Value::Map(map) => Some(map),
            _ => None,
        }
    }

    /// Returns a reference to the record if this is a `Record`, otherwise `None`.
    pub fn as_record(&self) -> Option<&GenericRecord<'a>> {
        match self {
            Value::Record(rec) => Some(rec),
            _ => None,
        }
    }
}
