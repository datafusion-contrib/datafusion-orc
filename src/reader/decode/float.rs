use snafu::ResultExt;

use crate::error::{self, Result};

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
