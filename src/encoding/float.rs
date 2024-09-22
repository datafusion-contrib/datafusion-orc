// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::marker::PhantomData;

use arrow::datatypes::{ArrowPrimitiveType, ToByteSlice};
use bytes::{Bytes, BytesMut};
use snafu::ResultExt;

use crate::{
    error::{self, Result},
    memory::EstimateMemory,
};

use super::{PrimitiveValueDecoder, PrimitiveValueEncoder};

/// Generically represent f32 and f64.
// TODO: figure out how to use num::traits::FromBytes instead of rolling our own?
pub trait Float: num::Float + std::fmt::Debug + num::traits::ToBytes {
    /// Named OBytes to not conflict with Bytes from [`num::traits::ToBytes`]
    type OBytes: AsRef<[u8]> + AsMut<[u8]> + Default;

    fn from_le_bytes(bytes: Self::OBytes) -> Self;
}

impl Float for f32 {
    type OBytes = [u8; 4];

    #[inline]
    fn from_le_bytes(bytes: Self::OBytes) -> Self {
        Self::from_le_bytes(bytes)
    }
}

impl Float for f64 {
    type OBytes = [u8; 8];

    #[inline]
    fn from_le_bytes(bytes: Self::OBytes) -> Self {
        Self::from_le_bytes(bytes)
    }
}

/// An iterator
pub struct FloatIter<T: Float, R: std::io::Read> {
    reader: R,
    phantom: std::marker::PhantomData<T>,
}

impl<T: Float, R: std::io::Read> FloatIter<T, R> {
    /// Returns a new [`FloatIter`]
    #[inline]
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            phantom: Default::default(),
        }
    }
}

impl<T: Float, R: std::io::Read> PrimitiveValueDecoder<T> for FloatIter<T, R> {}

impl<T: Float, R: std::io::Read> Iterator for FloatIter<T, R> {
    type Item = Result<T>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let mut chunk: T::OBytes = Default::default();
        match self
            .reader
            .read(chunk.as_mut())
            .context(error::DecodeFloatSnafu)
        {
            Err(err) => {
                return Some(Err(err));
            }
            Ok(n) => {
                if n == 0 {
                    return None;
                }
            }
        };
        Some(Ok(T::from_le_bytes(chunk)))
    }
}

/// No special run encoding for floats/doubles, they are stored as their IEEE 754 floating
/// point bit layout. This encoder simply copies incoming floats/doubles to its internal
/// byte buffer.
pub struct FloatValueEncoder<T: ArrowPrimitiveType>
where
    T::Native: Float,
{
    data: BytesMut,
    _phantom: PhantomData<T>,
}

impl<T: ArrowPrimitiveType> EstimateMemory for FloatValueEncoder<T>
where
    T::Native: Float,
{
    fn estimate_memory_size(&self) -> usize {
        self.data.len()
    }
}

impl<T: ArrowPrimitiveType> PrimitiveValueEncoder<T::Native> for FloatValueEncoder<T>
where
    T::Native: Float,
{
    fn new() -> Self {
        Self {
            data: BytesMut::new(),
            _phantom: Default::default(),
        }
    }

    fn write_one(&mut self, value: T::Native) {
        let bytes = value.to_byte_slice();
        self.data.extend_from_slice(bytes);
    }

    fn write_slice(&mut self, values: &[T::Native]) {
        let bytes = values.to_byte_slice();
        self.data.extend_from_slice(bytes)
    }

    fn take_inner(&mut self) -> Bytes {
        std::mem::take(&mut self.data).into()
    }
}

#[cfg(test)]
mod tests {
    use std::f32::consts as f32c;
    use std::f64::consts as f64c;
    use std::io::Cursor;

    use super::*;

    fn float_to_bytes<F: Float>(input: &[F]) -> Vec<u8> {
        input
            .iter()
            .flat_map(|f| f.to_le_bytes().as_ref().to_vec())
            .collect()
    }

    fn assert_roundtrip<F: Float>(input: Vec<F>) {
        let bytes = float_to_bytes(&input);
        let bytes = Cursor::new(bytes);

        let iter = FloatIter::<F, _>::new(bytes);
        let actual = iter.collect::<Result<Vec<_>>>().unwrap();

        assert_eq!(input, actual);
    }

    #[test]
    fn test_float_iter_empty() {
        assert_roundtrip::<f32>(vec![]);
    }

    #[test]
    fn test_double_iter_empty() {
        assert_roundtrip::<f64>(vec![]);
    }

    #[test]
    fn test_float_iter_one() {
        assert_roundtrip(vec![f32c::PI]);
    }

    #[test]
    fn test_double_iter_one() {
        assert_roundtrip(vec![f64c::PI]);
    }

    #[test]
    fn test_float_iter_nan() {
        let bytes = float_to_bytes(&[f32::NAN]);
        let bytes = Cursor::new(bytes);

        let iter = FloatIter::<f32, _>::new(bytes);
        let actual = iter.collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(actual.len(), 1);
        assert!(actual[0].is_nan());
    }

    #[test]
    fn test_double_iter_nan() {
        let bytes = float_to_bytes(&[f64::NAN]);
        let bytes = Cursor::new(bytes);

        let iter = FloatIter::<f64, _>::new(bytes);
        let actual = iter.collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(actual.len(), 1);
        assert!(actual[0].is_nan());
    }

    #[test]
    fn test_float_iter_many() {
        assert_roundtrip(vec![
            f32::NEG_INFINITY,
            f32::MIN,
            -1.0,
            -0.0,
            0.0,
            1.0,
            f32c::SQRT_2,
            f32::MAX,
            f32::INFINITY,
        ]);
    }

    #[test]
    fn test_double_iter_many() {
        assert_roundtrip(vec![
            f64::NEG_INFINITY,
            f64::MIN,
            -1.0,
            -0.0,
            0.0,
            1.0,
            f64c::SQRT_2,
            f64::MAX,
            f64::INFINITY,
        ]);
    }
}
