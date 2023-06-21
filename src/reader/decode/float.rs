use snafu::ResultExt;

use crate::error::{self, Result};

/// Sealead trait to generically represent f32 and f64.
pub trait Float: Default + Copy + private::Sealed {
    type Bytes: AsRef<[u8]> + AsMut<[u8]> + Default;
    fn from_le_bytes(bytes: Self::Bytes) -> Self;
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
}

impl Float for f64 {
    type Bytes = [u8; 8];

    #[inline]
    fn from_le_bytes(bytes: Self::Bytes) -> Self {
        Self::from_le_bytes(bytes)
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
