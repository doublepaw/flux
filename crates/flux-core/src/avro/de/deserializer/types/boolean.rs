// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

use super::*;

pub(in super::super) fn read_bool<'de, R, V>(
    state: &mut DeserializerState<'_, R>,
    visitor: V,
) -> Result<V::Value, DeError>
where
    R: ReadSlice<'de>,
    V: Visitor<'de>,
{
    visitor.visit_bool(state.read_slice(1, |s: &[u8]| match s[0] {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(DeError::custom(format_args!(
            "Invalid byte value when deserializing boolean: {:?}",
            other
        ))),
    })?)
}
