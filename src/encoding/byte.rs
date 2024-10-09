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

use bytemuck::must_cast_slice;
use bytes::{BufMut, BytesMut};
use snafu::ResultExt;

use crate::{
    error::{IoSnafu, Result},
    memory::EstimateMemory,
};
use std::io::Read;

use super::{rle::GenericRle, util::read_u8, PrimitiveValueEncoder};

const MAX_LITERAL_LENGTH: usize = 128;
const MIN_REPEAT_LENGTH: usize = 3;
const MAX_REPEAT_LENGTH: usize = 130;

pub struct ByteRleEncoder {
    writer: BytesMut,
    /// Literal values to encode.
    literals: [u8; MAX_LITERAL_LENGTH],
    /// Represents the number of elements currently in `literals` if Literals,
    /// otherwise represents the length of the Run.
    num_literals: usize,
    /// Tracks if current Literal sequence will turn into a Run sequence due to
    /// repeated values at the end of the value sequence.
    tail_run_length: usize,
    /// If in Run sequence or not, and keeps the corresponding value.
    run_value: Option<u8>,
}

impl ByteRleEncoder {
    /// Incrementally encode bytes using Run Length Encoding, where the subencodings are:
    ///   - Run: at least 3 repeated values in sequence (up to `MAX_REPEAT_LENGTH`)
    ///   - Literals: disparate values (up to `MAX_LITERAL_LENGTH` length)
    ///
    /// How the relevant encodings are chosen:
    ///   - Keep of track of values as they come, starting off assuming Literal sequence
    ///   - Keep track of latest value, to see if we are encountering a sequence of repeated
    ///     values (Run sequence)
    ///   - If this tail end exceeds the required minimum length, flush the current Literal
    ///     sequence (or switch to Run if entire current sequence is the repeated value)
    ///   - Whether in Literal or Run mode, keep buffering values and flushing when max length
    ///     reached or encoding is broken (e.g. non-repeated value found in Run mode)
    fn process_value(&mut self, value: u8) {
        // Adapted from https://github.com/apache/orc/blob/main/java/core/src/java/org/apache/orc/impl/RunLengthByteWriter.java
        if self.num_literals == 0 {
            // Start off in Literal mode
            self.run_value = None;
            self.literals[0] = value;
            self.num_literals = 1;
            self.tail_run_length = 1;
        } else if let Some(run_value) = self.run_value {
            // Run mode

            if value == run_value {
                // Continue buffering for Run sequence, flushing if reaching max length
                self.num_literals += 1;
                if self.num_literals == MAX_REPEAT_LENGTH {
                    write_run(&mut self.writer, run_value, MAX_REPEAT_LENGTH);
                    self.clear_state();
                }
            } else {
                // Run is broken, flush then start again in Literal mode
                write_run(&mut self.writer, run_value, self.num_literals);
                self.run_value = None;
                self.literals[0] = value;
                self.num_literals = 1;
                self.tail_run_length = 1;
            }
        } else {
            // Literal mode

            // tail_run_length tracks length of repetition of last value
            if value == self.literals[self.num_literals - 1] {
                self.tail_run_length += 1;
            } else {
                self.tail_run_length = 1;
            }

            if self.tail_run_length == MIN_REPEAT_LENGTH {
                // When the tail end of the current sequence is enough for a Run sequence

                if self.num_literals + 1 == MIN_REPEAT_LENGTH {
                    // If current values are enough for a Run sequence, switch to Run encoding
                    self.run_value = Some(value);
                    self.num_literals += 1;
                } else {
                    // Flush the current Literal sequence, then switch to Run encoding
                    // We don't flush the tail end which is a Run sequence
                    let len = self.num_literals - (MIN_REPEAT_LENGTH - 1);
                    let literals = &self.literals[..len];
                    write_literals(&mut self.writer, literals);
                    self.run_value = Some(value);
                    self.num_literals = MIN_REPEAT_LENGTH;
                }
            } else {
                // Continue buffering for Literal sequence, flushing if reaching max length
                self.literals[self.num_literals] = value;
                self.num_literals += 1;
                if self.num_literals == MAX_LITERAL_LENGTH {
                    // Entire literals is filled, pass in as is
                    write_literals(&mut self.writer, &self.literals);
                    self.clear_state();
                }
            }
        }
    }

    fn clear_state(&mut self) {
        self.run_value = None;
        self.tail_run_length = 0;
        self.num_literals = 0;
    }

    /// Flush any buffered values to writer in appropriate sequence.
    fn flush(&mut self) {
        if self.num_literals != 0 {
            if let Some(value) = self.run_value {
                write_run(&mut self.writer, value, self.num_literals);
            } else {
                let literals = &self.literals[..self.num_literals];
                write_literals(&mut self.writer, literals);
            }
            self.clear_state();
        }
    }
}

impl EstimateMemory for ByteRleEncoder {
    fn estimate_memory_size(&self) -> usize {
        self.writer.len() + self.num_literals
    }
}

/// i8 to match with Arrow Int8 type.
impl PrimitiveValueEncoder<i8> for ByteRleEncoder {
    fn new() -> Self {
        Self {
            writer: BytesMut::new(),
            literals: [0; MAX_LITERAL_LENGTH],
            num_literals: 0,
            tail_run_length: 0,
            run_value: None,
        }
    }

    fn write_one(&mut self, value: i8) {
        self.process_value(value as u8);
    }

    fn take_inner(&mut self) -> bytes::Bytes {
        self.flush();
        std::mem::take(&mut self.writer).into()
    }
}

fn write_run(writer: &mut BytesMut, value: u8, run_length: usize) {
    debug_assert!(
        (MIN_REPEAT_LENGTH..=MAX_REPEAT_LENGTH).contains(&run_length),
        "Byte RLE Run sequence must be in range 3..=130"
    );
    // [3, 130] to [0, 127]
    let header = run_length - MIN_REPEAT_LENGTH;
    writer.put_u8(header as u8);
    writer.put_u8(value);
}

fn write_literals(writer: &mut BytesMut, literals: &[u8]) {
    debug_assert!(
        (1..=MAX_LITERAL_LENGTH).contains(&literals.len()),
        "Byte RLE Literal sequence must be in range 1..=128"
    );
    // [1, 128] to [-1, -128], then writing as a byte
    let header = -(literals.len() as i32);
    writer.put_u8(header as u8);
    writer.put_slice(literals);
}

pub struct ByteRleDecoder<R> {
    reader: R,
    /// Values that have been decoded but not yet emitted.
    leftovers: Vec<u8>,
    /// Index into leftovers to make it act like a queue; indicates the
    /// next element available to read
    index: usize,
}

impl<R: Read> ByteRleDecoder<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            leftovers: Vec::with_capacity(MAX_REPEAT_LENGTH),
            index: 0,
        }
    }
}

impl<R: Read> GenericRle<i8> for ByteRleDecoder<R> {
    fn advance(&mut self, n: usize) {
        self.index += n
    }

    fn available(&self) -> &[i8] {
        let bytes = &self.leftovers[self.index..];
        must_cast_slice(bytes)
    }

    fn decode_batch(&mut self) -> Result<()> {
        self.index = 0;
        self.leftovers.clear();

        let header = read_u8(&mut self.reader)?;
        if header < 0x80 {
            // Run of repeated value
            let length = header as usize + MIN_REPEAT_LENGTH;
            let value = read_u8(&mut self.reader)?;
            self.leftovers.extend(std::iter::repeat(value).take(length));
        } else {
            // List of values
            let length = 0x100 - header as usize;
            self.leftovers.resize(length, 0);
            self.reader
                .read_exact(&mut self.leftovers)
                .context(IoSnafu)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::encoding::PrimitiveValueDecoder;

    use super::*;

    use proptest::prelude::*;

    // TODO: have tests varying the out buffer, to ensure decode() is called
    //       multiple times

    fn test_helper(data: &[u8], expected: &[i8]) {
        let mut reader = ByteRleDecoder::new(Cursor::new(data));
        let mut actual = vec![0; expected.len()];
        reader.decode(&mut actual).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn reader_test() {
        let data = [0x61u8, 0x00];
        let expected = [0; 100];
        test_helper(&data, &expected);

        let data = [0x01, 0x01];
        let expected = [1; 4];
        test_helper(&data, &expected);

        let data = [0xfe, 0x44, 0x45];
        let expected = [0x44, 0x45];
        test_helper(&data, &expected);
    }

    fn roundtrip_byte_rle_helper(values: &[i8]) -> Result<Vec<i8>> {
        let mut writer = ByteRleEncoder::new();
        writer.write_slice(values);
        writer.flush();

        let buf = writer.take_inner();
        let mut cursor = Cursor::new(&buf);
        let mut reader = ByteRleDecoder::new(&mut cursor);
        let mut actual = vec![0; values.len()];
        reader.decode(&mut actual)?;
        Ok(actual)
    }

    #[derive(Debug, Clone)]
    enum ByteSequence {
        Run(i8, usize),
        Literals(Vec<i8>),
    }

    fn byte_sequence_strategy() -> impl Strategy<Value = ByteSequence> {
        // We limit the max length of the sequences to 140 to try get more interleaving
        prop_oneof![
            (any::<i8>(), 1..140_usize).prop_map(|(a, b)| ByteSequence::Run(a, b)),
            prop::collection::vec(any::<i8>(), 1..140).prop_map(ByteSequence::Literals)
        ]
    }

    fn generate_bytes_from_sequences(sequences: Vec<ByteSequence>) -> Vec<i8> {
        let mut bytes = vec![];
        for sequence in sequences {
            match sequence {
                ByteSequence::Run(value, length) => {
                    bytes.extend(std::iter::repeat(value).take(length))
                }
                ByteSequence::Literals(literals) => bytes.extend(literals),
            }
        }
        bytes
    }

    proptest! {
        #[test]
        fn roundtrip_byte_rle_pure_random(values: Vec<i8>) {
            // Biased towards literal sequences due to purely random values
            let out = roundtrip_byte_rle_helper(&values).unwrap();
            prop_assert_eq!(out, values);
        }

        #[test]
        fn roundtrip_byte_rle_biased(
            sequences in prop::collection::vec(byte_sequence_strategy(), 1..200)
        ) {
            // Intentionally introduce run sequences to not be entirely random literals
            let values = generate_bytes_from_sequences(sequences);
            let out = roundtrip_byte_rle_helper(&values).unwrap();
            prop_assert_eq!(out, values);
        }
    }
}
