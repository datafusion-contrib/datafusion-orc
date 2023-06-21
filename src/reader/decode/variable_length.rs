use std::io::Read;

use snafu::ResultExt;

use crate::error::{self, Result};

pub struct Values<R: Read> {
    reader: R,
    scratch: Vec<u8>,
}

impl<R: Read> Values<R> {
    pub fn new(reader: R, scratch: Vec<u8>) -> Self {
        Self { reader, scratch }
    }

    pub fn next(&mut self, length: usize) -> Result<&[u8]> {
        self.scratch.clear();
        self.scratch.reserve(length);
        (&mut self.reader)
            .take(length as u64)
            .read_to_end(&mut self.scratch)
            .context(error::IoSnafu)?;

        Ok(&self.scratch)
    }

    pub fn into_inner(self) -> Vec<u8> {
        self.scratch
    }
}
