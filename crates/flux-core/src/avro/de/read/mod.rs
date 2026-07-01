// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Abstract reading from slices (propagating lifetime) or any other
//! `impl BufRead`/`impl Read` behind the same interface
//!
//! The deserializer is implemented generically on this.

pub mod take;

use super::{DeError, Error};

use integer_encoding::{VarInt, VarIntReader};

/// Inline optimized varint decoder for i64 (zigzag encoded).
/// Returns (value, bytes_consumed) or None if EOF.
#[inline(always)]
fn decode_varint_i64(slice: &[u8]) -> Option<(i64, usize)> {
    // Fast path: single byte (values -64 to 63)
    let b0 = *slice.first()?;
    if b0 < 0x80 {
        let unsigned = b0 as u64;
        let signed = ((unsigned >> 1) as i64) ^ -((unsigned & 1) as i64);
        return Some((signed, 1));
    }

    // Fast path: two bytes (values -8192 to 8191)
    let b1 = *slice.get(1)?;
    if b1 < 0x80 {
        let unsigned = ((b0 & 0x7f) as u64) | ((b1 as u64) << 7);
        let signed = ((unsigned >> 1) as i64) ^ -((unsigned & 1) as i64);
        return Some((signed, 2));
    }

    // Fast path: three bytes (values up to ~1M)
    let b2 = *slice.get(2)?;
    if b2 < 0x80 {
        let unsigned = ((b0 & 0x7f) as u64) | (((b1 & 0x7f) as u64) << 7) | ((b2 as u64) << 14);
        let signed = ((unsigned >> 1) as i64) ^ -((unsigned & 1) as i64);
        return Some((signed, 3));
    }

    // Slow path: 4+ bytes
    decode_varint_i64_slow(slice)
}

/// Slow path for varints with 4+ bytes
#[inline(never)]
fn decode_varint_i64_slow(slice: &[u8]) -> Option<(i64, usize)> {
    // We know first 3 bytes have MSB set
    if slice.len() < 4 {
        return None;
    }
    let b0 = slice[0] as u64;
    let b1 = slice[1] as u64;
    let b2 = slice[2] as u64;
    let b3 = slice[3] as u64;

    if b3 < 0x80 {
        let unsigned = (b0 & 0x7f) | ((b1 & 0x7f) << 7) | ((b2 & 0x7f) << 14) | (b3 << 21);
        let signed = ((unsigned >> 1) as i64) ^ -((unsigned & 1) as i64);
        return Some((signed, 4));
    }

    if slice.len() < 5 {
        return None;
    }
    let b4 = slice[4] as u64;
    if b4 < 0x80 {
        let unsigned = (b0 & 0x7f)
            | ((b1 & 0x7f) << 7)
            | ((b2 & 0x7f) << 14)
            | ((b3 & 0x7f) << 21)
            | (b4 << 28);
        let signed = ((unsigned >> 1) as i64) ^ -((unsigned & 1) as i64);
        return Some((signed, 5));
    }

    // For 6+ bytes, fall back to loop
    decode_varint_i64_very_slow(slice)
}

/// Very slow path for 9+ byte varints (very large values)
#[inline(never)]
fn decode_varint_i64_very_slow(slice: &[u8]) -> Option<(i64, usize)> {
    let mut result: u64 = 0;
    let mut shift = 0u32;

    for (i, &byte) in slice.iter().enumerate() {
        if i >= 10 {
            return None;
        }
        result |= ((byte & 0x7f) as u64) << shift;
        if byte < 0x80 {
            let signed = ((result >> 1) as i64) ^ -((result & 1) as i64);
            return Some((signed, i + 1));
        }
        shift += 7;
    }
    None
}

/// Inline optimized varint decoder for i32 (zigzag encoded).
#[inline(always)]
fn decode_varint_i32(slice: &[u8]) -> Option<(i32, usize)> {
    // Fast path: single byte (values -64 to 63)
    let b0 = *slice.first()?;
    if b0 < 0x80 {
        let unsigned = b0 as u32;
        let signed = ((unsigned >> 1) as i32) ^ -((unsigned & 1) as i32);
        return Some((signed, 1));
    }

    // Fast path: two bytes (values -8192 to 8191)
    let b1 = *slice.get(1)?;
    if b1 < 0x80 {
        let unsigned = ((b0 & 0x7f) as u32) | ((b1 as u32) << 7);
        let signed = ((unsigned >> 1) as i32) ^ -((unsigned & 1) as i32);
        return Some((signed, 2));
    }

    // Fast path: three bytes
    let b2 = *slice.get(2)?;
    if b2 < 0x80 {
        let unsigned = ((b0 & 0x7f) as u32) | (((b1 & 0x7f) as u32) << 7) | ((b2 as u32) << 14);
        let signed = ((unsigned >> 1) as i32) ^ -((unsigned & 1) as i32);
        return Some((signed, 3));
    }

    // Slow path: 4+ bytes
    decode_varint_i32_slow(slice)
}

/// Slow path for i32 varints with 4+ bytes
#[inline(never)]
fn decode_varint_i32_slow(slice: &[u8]) -> Option<(i32, usize)> {
    let mut result: u32 = 0;
    let mut shift = 0u32;

    for (i, &byte) in slice.iter().enumerate() {
        if i >= 5 {
            return None;
        }
        result |= ((byte & 0x7f) as u32) << shift;
        if byte < 0x80 {
            let signed = ((result >> 1) as i32) ^ -((result & 1) as i32);
            return Some((signed, i + 1));
        }
        shift += 7;
    }
    None
}

/// Trait for fast varint decoding with specialized implementations for common types.
pub trait FastVarInt: VarInt {
    /// Decode a varint from a slice, returning the value and bytes consumed.
    fn decode_fast(slice: &[u8]) -> Option<(Self, usize)>;
}

impl FastVarInt for i32 {
    #[inline(always)]
    fn decode_fast(slice: &[u8]) -> Option<(Self, usize)> {
        decode_varint_i32(slice)
    }
}

impl FastVarInt for i64 {
    #[inline(always)]
    fn decode_fast(slice: &[u8]) -> Option<(Self, usize)> {
        decode_varint_i64(slice)
    }
}

// Fallback implementations for other VarInt types
macro_rules! impl_fast_varint_fallback {
	($($t:ty),*) => {
		$(
			impl FastVarInt for $t {
				#[inline(always)]
				fn decode_fast(slice: &[u8]) -> Option<(Self, usize)> {
					Self::decode_var(slice)
				}
			}
		)*
	}
}

impl_fast_varint_fallback!(i8, i16, u8, u16, u32, u64, usize, isize);

/// Abstracts reading from slices or any other `impl BufRead` behind the same
/// interface
///
/// The deserializer is implemented generically on this.
pub trait Read: std::io::Read + Sized + private::Sealed {
    /// Read an integer of type `I` from the underlying buffer using varint
    /// encoding
    ///
    /// Note that Avro uses signed integers all the time, so there is seldom
    /// use-case for unsigned integers here.
    fn read_varint<I>(&mut self) -> Result<I, DeError>
    where
        I: FastVarInt;
    /// Read a buffer of size `N` from the underlying buffer, returning it
    /// as an array. This is a convenience method because the deserializer often
    /// needs to run fixed-size buffers to immediately turn them into values.
    fn read_const_size_buf<const N: usize>(&mut self) -> Result<[u8; N], DeError> {
        let mut buf = [0u8; N];
        self.read_exact(&mut buf).map_err(DeError::io)?;
        Ok(buf)
    }
    /// Skip `n_bytes` bytes from the underlying buffer
    fn skip_bytes(&mut self, n_bytes: u64) -> Result<(), DeError> {
        let written = std::io::copy(
            &mut <&mut Self as std::io::Read>::take(self, n_bytes),
            &mut std::io::sink(),
        )
        .map_err(DeError::io)?;
        if written == n_bytes {
            Ok(())
        } else {
            Err(DeError::custom(format_args!(
                "Expected to skip {} bytes, but only skipped {}",
                n_bytes, written
            )))
        }
    }
}

/// Abstracts reading from slices (propagating lifetime) or any other `impl
/// Read` behind the same interface
///
/// The deserializer is implemented generically on this.
pub trait ReadSlice<'de>: Read {
    /// Read a slice of `n` bytes from the underlying buffer, and pass it to
    /// the visitor to turn it into a value
    fn read_slice<V>(&mut self, n: usize, read_visitor: V) -> Result<V::Value, DeError>
    where
        V: ReadVisitor<'de>;
}

mod private {
    pub trait Sealed {}
}

/// Implements `Read<'de>` reading from `&'de [u8]`
pub struct SliceRead<'de> {
    slice: &'de [u8],
}
impl<'de> SliceRead<'de> {
    /// Construct a `SliceRead` from a `&'de [u8]`
    pub fn new(slice: &'de [u8]) -> Self {
        Self { slice }
    }
}
impl private::Sealed for SliceRead<'_> {}
impl<'de> Read for SliceRead<'de> {
    fn read_varint<I>(&mut self) -> Result<I, DeError>
    where
        I: FastVarInt,
    {
        match I::decode_fast(self.slice) {
            None => Err(DeError::new(
                "All bytes have MSB set when decoding varint (Reached EOF)",
            )),
            Some((val, read)) => {
                self.slice = &self.slice[read..];
                Ok(val)
            }
        }
    }
    fn skip_bytes(&mut self, n_bytes: u64) -> Result<(), DeError> {
        let n_bytes: usize = n_bytes
            .try_into()
            .map_err(|_| DeError::custom("Invalid number of bytes to skip"))?;
        match self.slice.get(n_bytes..) {
            Some(rest) => {
                self.slice = rest;
                Ok(())
            }
            None => Err(DeError::unexpected_eof()),
        }
    }
}
impl<'de> ReadSlice<'de> for SliceRead<'de> {
    fn read_slice<V>(&mut self, n: usize, visitor: V) -> Result<V::Value, DeError>
    where
        V: ReadVisitor<'de>,
    {
        if n > self.slice.len() {
            Err(DeError::unexpected_eof())
        } else {
            let (just_read, end) = self.slice.split_at(n);
            self.slice = end;
            visitor.visit_borrowed(just_read)
        }
    }
}
impl std::io::Read for SliceRead<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.slice.read(buf)
    }
    fn read_vectored(&mut self, bufs: &mut [std::io::IoSliceMut<'_>]) -> std::io::Result<usize> {
        self.slice.read_vectored(bufs)
    }
}
impl std::io::BufRead for SliceRead<'_> {
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        self.slice.fill_buf()
    }

    fn consume(&mut self, amt: usize) {
        self.slice.consume(amt)
    }
}

/// Implements `Read<'de>` reading from any `impl BufRead`
pub struct ReaderRead<R> {
    reader: R,
    scratch: Vec<u8>,
    /// Maximum allocation size for a single field (string, bytes...)
    ///
    /// This is a safeguard for malformed data
    ///
    /// Default is 512 MB.
    ///
    /// See [`de`](crate::de) module documentation for an example of how to set
    /// this.
    pub max_alloc_size: usize,
}
impl<R: std::io::Read> private::Sealed for ReaderRead<R> {}
impl<R: std::io::BufRead> ReaderRead<R> {
    /// Construct a `ReaderRead` from an `impl BufRead`
    ///
    /// If you only have an `impl Read`, wrap it in a
    /// [`BufReader`](std::io::BufReader) first.
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            scratch: Vec::new(),
            max_alloc_size: 512 * 1024 * 1024,
        }
    }
}
impl<R> ReaderRead<R> {
    /// Consume the `ReaderRead` and return the inner reader
    pub fn into_inner(self) -> R {
        self.reader
    }
}
impl<R: std::io::BufRead> Read for ReaderRead<R> {
    fn read_varint<I>(&mut self) -> Result<I, DeError>
    where
        I: FastVarInt,
    {
        use std::io::BufRead;
        // Try to decode in one go from the buffer slice.
        // On buffer refill boundaries, that may fail, so we fall back to the
        // more general `read_varint` method that reads byte by byte (that's slightly
        // sub-optimal but also will trigger extremely rarely).
        match I::decode_fast(self.fill_buf().map_err(DeError::io)?) {
            None => <Self as VarIntReader>::read_varint(self).map_err(DeError::io),
            Some((val, read)) => {
                self.consume(read);
                Ok(val)
            }
        }
    }
}
impl<'de, R: std::io::BufRead> ReadSlice<'de> for ReaderRead<R> {
    fn read_slice<V>(&mut self, n: usize, read_visitor: V) -> Result<V::Value, DeError>
    where
        V: ReadVisitor<'de>,
    {
        let buffer = self.reader.fill_buf().map_err(DeError::io)?;
        match buffer.get(0..n) {
            Some(slice) => {
                let produced = read_visitor.visit(slice)?;
                self.reader.consume(n);
                Ok(produced)
            }
            None => {
                if n > self.max_alloc_size {
                    return Err(DeError::custom(format_args!(
                        "Allocation size that would be required ({n}) is larger than \
							allowed for this deserializer from reader ({}) - \
							this is probably due to malformed data",
                        self.max_alloc_size
                    )));
                }
                if n > self.scratch.len() {
                    self.scratch.resize(n, 0);
                }
                let scratch = &mut self.scratch[..n];
                self.reader.read_exact(scratch).map_err(DeError::io)?;
                read_visitor.visit(scratch)
            }
        }
    }
}
impl<R: std::io::Read> std::io::Read for ReaderRead<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.reader.read(buf)
    }
    fn read_vectored(&mut self, bufs: &mut [std::io::IoSliceMut<'_>]) -> std::io::Result<usize> {
        self.reader.read_vectored(bufs)
    }
}
impl<R: std::io::BufRead> std::io::BufRead for ReaderRead<R> {
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        self.reader.fill_buf()
    }

    fn consume(&mut self, amt: usize) {
        self.reader.consume(amt)
    }
}

/// Largely internal trait for `Read` usage (probably don't use this directly)
///
/// This is what can be passed to [`ReadSlice::read_slice`] to obtain either
/// owned or borrowed values depending on whether we're reading from a slice or
/// an arbitrary impl `Read`.
pub trait ReadVisitor<'de>: Sized {
    /// The value that this `Visitor` generates
    type Value;
    /// How to construct the `Value` from a short-lived slice
    fn visit(self, bytes: &[u8]) -> Result<Self::Value, DeError>;
    /// How to construct the `Value` from a borrowed slice
    fn visit_borrowed(self, bytes: &'de [u8]) -> Result<Self::Value, DeError> {
        self.visit(bytes)
    }
}

impl<'de, F, V> ReadVisitor<'de> for F
where
    F: FnOnce(&[u8]) -> Result<V, DeError>,
{
    type Value = V;
    fn visit(self, bytes: &[u8]) -> Result<Self::Value, DeError> {
        self(bytes)
    }
}
