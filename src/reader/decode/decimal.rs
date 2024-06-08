use std::io::Read;

use crate::{error::Result, reader::decode::util::read_varint_zigzagged};

use super::SignedEncoding;

/// Read stream of zigzag encoded varints as i128 (unbound).
pub struct UnboundedVarintStreamDecoder<R: Read> {
    reader: R,
    remaining: usize,
}

impl<R: Read> UnboundedVarintStreamDecoder<R> {
    pub fn new(reader: R, expected_length: usize) -> Self {
        Self {
            reader,
            remaining: expected_length,
        }
    }
}

impl<R: Read> Iterator for UnboundedVarintStreamDecoder<R> {
    type Item = Result<i128>;

    fn next(&mut self) -> Option<Self::Item> {
        (self.remaining > 0).then(|| {
            self.remaining -= 1;
            read_varint_zigzagged::<i128, _, SignedEncoding>(&mut self.reader)
        })
    }
}
