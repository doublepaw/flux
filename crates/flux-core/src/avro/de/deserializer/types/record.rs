// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

use super::*;

/// A deserializer specifically for records.
/// This is passed to visit_newtype_struct so Value deserialization knows it's a record.
pub(in super::super) struct RecordDeserializer<'r, 's, R> {
    pub(in super::super) state: &'r mut DeserializerState<'s, R>,
    pub(in super::super) record_fields: std::slice::Iter<'s, RecordField<'s>>,
    pub(in super::super) allowed_depth: AllowedDepth,
    pub(in super::super) field_count: usize,
}

impl<'de, R: ReadSlice<'de>> serde::Deserializer<'de> for RecordDeserializer<'_, '_, R> {
    type Error = DeError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_seq(RecordSeqAccess {
            state: self.state,
            record_fields: self.record_fields,
            allowed_depth: self.allowed_depth,
            field_count: self.field_count,
        })
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct map struct enum identifier ignored_any
    }
}

/// SeqAccess for deserializing records as sequences of field values.
/// This is faster than MapAccess because it doesn't deserialize field names.
pub(in super::super) struct RecordSeqAccess<'r, 's, R> {
    pub(in super::super) state: &'r mut DeserializerState<'s, R>,
    pub(in super::super) record_fields: std::slice::Iter<'s, RecordField<'s>>,
    pub(in super::super) allowed_depth: AllowedDepth,
    pub(in super::super) field_count: usize,
}

impl<'de, R: ReadSlice<'de>> SeqAccess<'de> for RecordSeqAccess<'_, '_, R> {
    type Error = DeError;

    #[inline(always)]
    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        match self.record_fields.next() {
            None => Ok(None),
            Some(field) => {
                let value = seed.deserialize(DatumDeserializer {
                    schema_node: field.schema.as_ref(),
                    state: self.state,
                    allowed_depth: self.allowed_depth,
                })?;
                Ok(Some(value))
            }
        }
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.field_count)
    }
}
