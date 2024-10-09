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

use crate::error::{OutOfSpecSnafu, Result};

use super::PrimitiveValueDecoder;

mod sealed {
    use std::io::Read;

    use crate::encoding::{
        byte::ByteRleDecoder,
        integer::{rle_v1::RleV1Decoder, rle_v2::RleV2Decoder, EncodingSign, NInt},
    };

    pub trait Rle {}

    impl<R: Read> Rle for ByteRleDecoder<R> {}
    impl<N: NInt, R: Read, S: EncodingSign> Rle for RleV1Decoder<N, R, S> {}
    impl<N: NInt, R: Read, S: EncodingSign> Rle for RleV2Decoder<N, R, S> {}
}

/// Generic decoding behaviour for run length encoded values, such as integers (v1 and v2)
/// and bytes.
///
/// Assumes an internal buffer which acts like a (single headed) queue where values are first
/// decoded into, before being copied out into the output buffer (usually an Arrow array).
pub trait GenericRle<V: Copy> {
    /// Consume N elements from internal buffer to signify the values having been copied out.
    fn advance(&mut self, n: usize);

    /// All values available in internal buffer, respecting the current advancement level.
    fn available(&self) -> &[V];

    /// This should clear the internal buffer and populate it with the next round of decoded
    /// values.
    // TODO: Have a version that copies directly into the output buffer (e.g. Arrow array).
    //       Currently we always decode to the internal buffer first, even if we can copy
    //       directly to the output and skip the middle man. Ideally the internal buffer
    //       should only be used for leftovers between calls to PrimitiveValueDecoder::decode.
    fn decode_batch(&mut self) -> Result<()>;
}

impl<V: Copy, G: GenericRle<V> + sealed::Rle> PrimitiveValueDecoder<V> for G {
    fn decode(&mut self, out: &mut [V]) -> Result<()> {
        let available = self.available();
        // If we have enough leftover to copy, can skip decoding more.
        if available.len() >= out.len() {
            out.copy_from_slice(&available[..out.len()]);
            self.advance(out.len());
            return Ok(());
        }

        // Otherwise progressively decode and copy over chunks.
        let len_to_copy = out.len();
        let mut copied = 0;
        while copied < len_to_copy {
            if self.available().is_empty() {
                self.decode_batch()?;
            }

            let copying = self.available().len();
            // At most, we fill to exact length of output buffer (don't overflow).
            let copying = copying.min(len_to_copy - copied);

            let out = &mut out[copied..];
            out[..copying].copy_from_slice(&self.available()[..copying]);

            copied += copying;
            self.advance(copying);
        }

        // We always expect to be able to fill the output buffer; it is up to the
        // caller to control that size.
        if copied != out.len() {
            // TODO: more descriptive error
            OutOfSpecSnafu {
                msg: "Array length less than expected",
            }
            .fail()
        } else {
            Ok(())
        }
    }
}
