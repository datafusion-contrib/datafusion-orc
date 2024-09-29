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

use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanBufferBuilder, UnionArray};
use arrow::buffer::{Buffer, NullBuffer};
use arrow::datatypes::UnionFields;
use snafu::ResultExt;

use crate::column::Column;
use crate::encoding::byte::ByteRleDecoder;
use crate::encoding::PrimitiveValueDecoder;
use crate::error::ArrowSnafu;
use crate::error::Result;
use crate::proto::stream::Kind;
use crate::stripe::Stripe;

use super::{array_decoder_factory, derive_present_vec, ArrayBatchDecoder, PresentDecoder};

/// Decode ORC Union column into batches of Arrow Sparse UnionArrays.
pub struct UnionArrayDecoder {
    // fields and variants should have same length
    // TODO: encode this assumption into types
    fields: UnionFields,
    variants: Vec<Box<dyn ArrayBatchDecoder>>,
    tags: Box<dyn PrimitiveValueDecoder<i8> + Send>,
    present: Option<PresentDecoder>,
}

impl UnionArrayDecoder {
    pub fn new(column: &Column, fields: UnionFields, stripe: &Stripe) -> Result<Self> {
        let present = PresentDecoder::from_stripe(stripe, column);

        let tags = stripe.stream_map().get(column, Kind::Data);
        let tags = Box::new(ByteRleDecoder::new(tags));

        let variants = column
            .children()
            .iter()
            .zip(fields.iter())
            .map(|(child, (_id, field))| array_decoder_factory(child, field.clone(), stripe))
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            fields,
            variants,
            tags,
            present,
        })
    }
}

impl ArrayBatchDecoder for UnionArrayDecoder {
    fn next_batch(
        &mut self,
        batch_size: usize,
        parent_present: Option<&NullBuffer>,
    ) -> Result<ArrayRef> {
        let present =
            derive_present_vec(&mut self.present, parent_present, batch_size).transpose()?;
        let mut tags = vec![0; batch_size];
        match &present {
            Some(present) => {
                // Since UnionArrays don't have nullability, we rely on child arrays.
                // So we default to first child (tag 0) for any nulls from this parent Union.
                self.tags.decode_spaced(&mut tags, present)?;
            }
            None => {
                self.tags.decode(&mut tags)?;
            }
        }

        // Calculate nullability for children
        let mut children_nullability = (0..self.variants.len())
            .map(|index| {
                let mut child_present = BooleanBufferBuilder::new(batch_size);
                child_present.append_n(batch_size, false);
                for idx in tags
                    .iter()
                    .enumerate()
                    // Where the parent expects the value of the child, we set to non-null.
                    // Otherwise for the sparse spots, we leave as null in children.
                    .filter_map(|(idx, &tag)| (tag as usize == index).then_some(idx))
                {
                    child_present.set_bit(idx, true);
                }
                child_present
            })
            .collect::<Vec<_>>();
        // If parent says a slot is null, we need to ensure the first child (0-index) also
        // encodes this information, since as mentioned before, Arrow UnionArrays don't store
        // nullability and rely on their children. We default to first child to encode this
        // information so need to enforce that here.
        if let Some(present) = &present {
            let first_child = &mut children_nullability[0];
            for idx in present
                .iter()
                .enumerate()
                .filter_map(|(idx, parent_present)| (!parent_present).then_some(idx))
            {
                first_child.set_bit(idx, false);
            }
        }

        let child_arrays = self
            .variants
            .iter_mut()
            .zip(children_nullability)
            .map(|(decoder, mut present)| {
                let present = NullBuffer::from(present.finish());
                decoder.next_batch(batch_size, Some(&present))
            })
            .collect::<Result<Vec<_>>>()?;

        // Currently default to decoding as Sparse UnionArray so no value offsets
        let type_ids = Buffer::from_vec(tags.clone()).into();
        let array = UnionArray::try_new(self.fields.clone(), type_ids, None, child_arrays)
            .context(ArrowSnafu)?;
        let array = Arc::new(array);
        Ok(array)
    }
}
