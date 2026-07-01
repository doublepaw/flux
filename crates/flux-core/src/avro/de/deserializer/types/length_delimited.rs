// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

use super::*;

pub(super) fn read_len<'de, R>(state: &mut DeserializerState<'_, R>) -> Result<usize, DeError>
where
    R: ReadSlice<'de>,
{
    state
        .read_varint::<i64>()?
        .try_into()
        .map_err(|e| DeError::custom(format_args!("Invalid buffer length in stream: {e}")))
}

pub(in super::super) fn read_length_delimited<'de, R, BV>(
    state: &mut DeserializerState<'_, R>,
    visitor: BV,
) -> Result<BV::Value, DeError>
where
    R: ReadSlice<'de>,
    BV: ReadVisitor<'de>,
{
    let len = read_len(state)?;
    state.read_slice(len, visitor)
}

pub(in super::super) struct BytesVisitor<V>(pub(in super::super) V);
impl<'de, V: Visitor<'de>> ReadVisitor<'de> for BytesVisitor<V> {
    type Value = V::Value;
    fn visit(self, bytes: &[u8]) -> Result<Self::Value, DeError> {
        self.0.visit_bytes(bytes)
    }
    fn visit_borrowed(self, bytes: &'de [u8]) -> Result<Self::Value, DeError> {
        self.0.visit_borrowed_bytes(bytes)
    }
}

pub(in super::super) struct StringVisitor<V>(pub(in super::super) V);
impl<'de, V: Visitor<'de>> ReadVisitor<'de> for StringVisitor<V> {
    type Value = V::Value;
    fn visit(self, bytes: &[u8]) -> Result<Self::Value, DeError> {
        self.0.visit_str(parse_str(bytes))
    }
    fn visit_borrowed(self, bytes: &'de [u8]) -> Result<Self::Value, DeError> {
        self.0.visit_borrowed_str(parse_str(bytes))
    }
}

/// Parse bytes as UTF-8 string without validation.
///
/// # Safety
/// This uses `from_utf8_unchecked` for performance. Avro strings are required
/// to be valid UTF-8 by the spec, so this is safe for well-formed Avro data.
#[inline]
fn parse_str(bytes: &[u8]) -> &str {
    // SAFETY: Avro spec requires strings to be valid UTF-8.
    // For untrusted input, use a validating deserializer.
    unsafe { std::str::from_utf8_unchecked(bytes) }
}
