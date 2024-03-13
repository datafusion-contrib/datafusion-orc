use snafu::ResultExt;

use crate::error::{self, Result};

/// Sealead trait to generically represent f32 and f64.
pub trait Float: num::Float + private::Sealed + std::fmt::Debug {
    type Bytes: AsRef<[u8]> + AsMut<[u8]> + Default;

    fn from_le_bytes(bytes: Self::Bytes) -> Self;

    fn to_le_bytes(&self) -> Self::Bytes;
}

mod private {
    pub trait Sealed {} // Users in other crates cannot name this trait.
    impl Sealed for f32 {}
    impl Sealed for f64 {}
}

impl Float for f32 {
    type Bytes = [u8; 4];

    #[inline]
    fn from_le_bytes(bytes: Self::Bytes) -> Self {
        Self::from_le_bytes(bytes)
    }

    #[inline]
    fn to_le_bytes(&self) -> Self::Bytes {
        Self::to_le_bytes(*self)
    }
}

impl Float for f64 {
    type Bytes = [u8; 8];

    #[inline]
    fn from_le_bytes(bytes: Self::Bytes) -> Self {
        Self::from_le_bytes(bytes)
    }

    #[inline]
    fn to_le_bytes(&self) -> Self::Bytes {
        Self::to_le_bytes(*self)
    }
}

/// An iterator
pub struct FloatIter<T: Float, R: std::io::Read> {
    reader: R,
    remaining: usize,
    phantom: std::marker::PhantomData<T>,
}

impl<T: Float, R: std::io::Read> FloatIter<T, R> {
    /// Returns a new [`FloatIter`]
    #[inline]
    pub fn new(reader: R, length: usize) -> Self {
        Self {
            reader,
            remaining: length,
            phantom: Default::default(),
        }
    }

    /// The number of items remaining
    #[inline]
    pub fn len(&self) -> usize {
        self.remaining
    }

    /// Whether the iterator is empty
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns its internal reader
    pub fn into_inner(self) -> R {
        self.reader
    }
}

impl<T: Float, R: std::io::Read> Iterator for FloatIter<T, R> {
    type Item = Result<T>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let mut chunk: T::Bytes = Default::default();
        if let Err(err) = self
            .reader
            .read_exact(chunk.as_mut())
            .context(error::DecodeFloatSnafu)
        {
            return Some(Err(err));
        };
        self.remaining -= 1;
        Some(Ok(T::from_le_bytes(chunk)))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.len();
        (remaining, Some(remaining))
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

        let iter = FloatIter::<F, _>::new(bytes, input.len());
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

        let iter = FloatIter::<f32, _>::new(bytes, 1);
        let actual = iter.collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(actual.len(), 1);
        assert!(actual[0].is_nan());
    }

    #[test]
    fn test_double_iter_nan() {
        let bytes = float_to_bytes(&[f64::NAN]);
        let bytes = Cursor::new(bytes);

        let iter = FloatIter::<f64, _>::new(bytes, 1);
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
