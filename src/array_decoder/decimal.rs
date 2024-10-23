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

use std::cmp::Ordering;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::buffer::NullBuffer;
use arrow::datatypes::Decimal128Type;
use snafu::ResultExt;

use crate::encoding::decimal::UnboundedVarintStreamDecoder;
use crate::encoding::integer::get_rle_reader;
use crate::encoding::PrimitiveValueDecoder;
use crate::error::ArrowSnafu;
use crate::proto::stream::Kind;
use crate::stripe::Stripe;
use crate::{column::Column, error::Result};

use super::{ArrayBatchDecoder, PresentDecoder, PrimitiveArrayDecoder};

pub fn new_decimal_decoder(
    column: &Column,
    stripe: &Stripe,
    precision: u32,
    fixed_scale: u32,
) -> Result<Box<dyn ArrayBatchDecoder>> {
    let varint_iter = stripe.stream_map().get(column, Kind::Data);
    let varint_iter = Box::new(UnboundedVarintStreamDecoder::new(varint_iter));

    // Scale is specified on a per varint basis (in addition to being encoded in the type)
    let scale_iter = stripe.stream_map().get(column, Kind::Secondary);
    let scale_iter = get_rle_reader::<i32, _>(column, scale_iter)?;

    let present = PresentDecoder::from_stripe(stripe, column);

    let iter = DecimalScaleRepairDecoder {
        varint_iter,
        scale_iter,
        fixed_scale,
    };
    let iter = Box::new(iter);

    Ok(Box::new(DecimalArrayDecoder::new(
        precision as u8,
        fixed_scale as i8,
        iter,
        present,
    )))
}

/// Wrapper around PrimitiveArrayDecoder to allow specifying the precision and scale
/// of the output decimal array.
pub struct DecimalArrayDecoder {
    precision: u8,
    scale: i8,
    inner: PrimitiveArrayDecoder<Decimal128Type>,
}

impl DecimalArrayDecoder {
    pub fn new(
        precision: u8,
        scale: i8,
        iter: Box<dyn PrimitiveValueDecoder<i128> + Send>,
        present: Option<PresentDecoder>,
    ) -> Self {
        let inner = PrimitiveArrayDecoder::<Decimal128Type>::new(iter, present);
        Self {
            precision,
            scale,
            inner,
        }
    }
}

impl ArrayBatchDecoder for DecimalArrayDecoder {
    fn next_batch(
        &mut self,
        batch_size: usize,
        parent_present: Option<&NullBuffer>,
    ) -> Result<ArrayRef> {
        let array = self
            .inner
            .next_primitive_batch(batch_size, parent_present)?
            .with_precision_and_scale(self.precision, self.scale)
            .context(ArrowSnafu)?;
        let array = Arc::new(array) as ArrayRef;
        Ok(array)
    }
}

/// This iter fixes the scales of the varints decoded as scale is specified on a per
/// varint basis, and needs to align with type specified scale
struct DecimalScaleRepairDecoder {
    varint_iter: Box<dyn PrimitiveValueDecoder<i128> + Send>,
    scale_iter: Box<dyn PrimitiveValueDecoder<i32> + Send>,
    fixed_scale: u32,
}

impl PrimitiveValueDecoder<i128> for DecimalScaleRepairDecoder {
    fn decode(&mut self, out: &mut [i128]) -> Result<()> {
        // TODO: can probably optimize, reuse buffers?
        let mut varint = vec![0; out.len()];
        let mut scale = vec![0; out.len()];
        self.varint_iter.decode(&mut varint)?;
        self.scale_iter.decode(&mut scale)?;
        for (index, (&varint, &scale)) in varint.iter().zip(scale.iter()).enumerate() {
            out[index] = fix_i128_scale(varint, self.fixed_scale, scale);
        }
        Ok(())
    }
}

fn fix_i128_scale(i: i128, fixed_scale: u32, varying_scale: i32) -> i128 {
    // TODO: Verify with C++ impl in ORC repo, which does this cast
    //       Not sure why scale stream can be signed if it gets casted to unsigned anyway
    //       https://github.com/apache/orc/blob/0014bec1e4cdd1206f5bae4f5c2000b9300c6eb1/c%2B%2B/src/ColumnReader.cc#L1459-L1476
    let varying_scale = varying_scale as u32;
    match fixed_scale.cmp(&varying_scale) {
        Ordering::Less => {
            // fixed_scale < varying_scale
            // Current scale of number is greater than scale of the array type
            // So need to divide to align the scale
            // TODO: this differs from C++ implementation, need to verify
            let scale_factor = varying_scale - fixed_scale;
            // TODO: replace with lookup table?
            let scale_factor = 10_i128.pow(scale_factor);
            i / scale_factor
        }
        Ordering::Equal => i,
        Ordering::Greater => {
            // fixed_scale > varying_scale
            // Current scale of number is smaller than scale of the array type
            // So need to multiply to align the scale
            // TODO: this differs from C++ implementation, need to verify
            let scale_factor = fixed_scale - varying_scale;
            // TODO: replace with lookup table?
            let scale_factor = 10_i128.pow(scale_factor);
            i * scale_factor
        }
    }
}
