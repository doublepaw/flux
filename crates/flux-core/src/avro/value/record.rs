// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! GenericRecord implementation for Avro records.
//!
//! Fields are stored as a Vec in schema order. Field name lookups require
//! the schema's field index map for O(1) access.

use super::Value;
use std::collections::HashMap;

use crate::avro::schema::FieldType;

/// A generic Avro record with fields stored in schema order.
///
/// This struct stores only the field values (no field names), making deserialization
/// fast by avoiding per-record HashMap construction and string allocations.
/// Field access by name requires the schema's field index map.
///
/// For high-performance deserialization, use [`BumpValue`](super::BumpValue) and
/// [`from_datum_slice_bump`](super::from_datum_slice_bump) which use arena allocation.
#[derive(Debug, Clone, PartialEq)]
pub struct GenericRecord<'a>(Vec<Value<'a>>);

impl<'a> GenericRecord<'a> {
    /// Create a new record with the specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self(Vec::with_capacity(capacity))
    }

    /// Create a new record from a vector of fields.
    pub fn from_fields(fields: Vec<Value<'a>>) -> Self {
        Self(fields)
    }

    /// Get a reference to a field value by name using the schema's field index.
    ///
    /// The `fields` parameter should be obtained from `schema.record_index().get_record_fields(record_name)`.
    ///
    /// Returns `None` if the field does not exist.
    /// This is an O(1) operation.
    pub fn get(
        &self,
        name: &str,
        fields: &HashMap<String, (usize, FieldType)>,
    ) -> Option<&Value<'a>> {
        let (idx, _) = fields.get(name)?;
        self.0.get(*idx)
    }

    /// Get a mutable reference to a field value by name.
    pub fn get_mut(
        &mut self,
        name: &str,
        fields: &HashMap<String, (usize, FieldType)>,
    ) -> Option<&mut Value<'a>> {
        let (idx, _) = fields.get(name)?;
        self.0.get_mut(*idx)
    }

    /// Get a reference to a field value by its index (position in schema).
    ///
    /// This is an O(1) operation that doesn't require the schema.
    pub fn get_by_index(&self, idx: usize) -> Option<&Value<'a>> {
        self.0.get(idx)
    }

    /// Get a mutable reference to a field value by its index.
    pub fn get_by_index_mut(&mut self, idx: usize) -> Option<&mut Value<'a>> {
        self.0.get_mut(idx)
    }

    /// Push a field value (appends to the end).
    ///
    /// Fields should be pushed in schema order during deserialization.
    pub fn push(&mut self, value: Value<'a>) {
        self.0.push(value);
    }

    /// Returns the number of fields in the record.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` if the record has no fields.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns an iterator over the field values in schema order.
    pub fn values(&self) -> impl Iterator<Item = &Value<'a>> {
        self.0.iter()
    }

    /// Returns an iterator over the field values in schema order (mutable).
    pub fn values_mut(&mut self) -> impl Iterator<Item = &mut Value<'a>> {
        self.0.iter_mut()
    }

    /// Convert this record into an owned version with `'static` lifetime.
    pub fn into_owned(self) -> GenericRecord<'static> {
        GenericRecord(self.0.into_iter().map(|v| v.into_owned()).collect())
    }
}

impl<'a> IntoIterator for GenericRecord<'a> {
    type Item = Value<'a>;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a, 'b> IntoIterator for &'b GenericRecord<'a> {
    type Item = &'b Value<'a>;
    type IntoIter = std::slice::Iter<'b, Value<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}
