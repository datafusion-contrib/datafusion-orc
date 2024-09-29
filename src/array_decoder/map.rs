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

use arrow::array::{ArrayRef, MapArray, StructArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{Field, Fields};
use snafu::ResultExt;

use crate::array_decoder::derive_present_vec;
use crate::column::Column;
use crate::encoding::integer::get_unsigned_rle_reader;
use crate::encoding::PrimitiveValueDecoder;
use crate::error::{ArrowSnafu, Result};
use crate::proto::stream::Kind;
use crate::stripe::Stripe;

use super::{array_decoder_factory, ArrayBatchDecoder, PresentDecoder};

pub struct MapArrayDecoder {
    keys: Box<dyn ArrayBatchDecoder>,
    values: Box<dyn ArrayBatchDecoder>,
    present: Option<PresentDecoder>,
    lengths: Box<dyn PrimitiveValueDecoder<i64> + Send>,
    fields: Fields,
}

impl MapArrayDecoder {
    pub fn new(
        column: &Column,
        keys_field: Arc<Field>,
        values_field: Arc<Field>,
        stripe: &Stripe,
    ) -> Result<Self> {
        let present = PresentDecoder::from_stripe(stripe, column);

        let keys_column = &column.children()[0];
        let keys = array_decoder_factory(keys_column, keys_field.clone(), stripe)?;

        let values_column = &column.children()[1];
        let values = array_decoder_factory(values_column, values_field.clone(), stripe)?;

        let reader = stripe.stream_map().get(column, Kind::Length);
        let lengths = get_unsigned_rle_reader(column, reader);

        let fields = Fields::from(vec![keys_field, values_field]);

        Ok(Self {
            keys,
            values,
            present,
            lengths,
            fields,
        })
    }
}

impl ArrayBatchDecoder for MapArrayDecoder {
    fn next_batch(
        &mut self,
        batch_size: usize,
        parent_present: Option<&NullBuffer>,
    ) -> Result<ArrayRef> {
        let present =
            derive_present_vec(&mut self.present, parent_present, batch_size).transpose()?;

        let mut lengths = vec![0; batch_size];
        if let Some(present) = &present {
            self.lengths.decode_spaced(&mut lengths, present)?;
        } else {
            self.lengths.decode(&mut lengths)?;
        }
        let total_length: i64 = lengths.iter().sum();
        // Fetch key and value arrays, each with total_length elements
        // Fetch child array as one Array with total_length elements
        let keys_array = self.keys.next_batch(total_length as usize, None)?;
        let values_array = self.values.next_batch(total_length as usize, None)?;
        // Compose the keys + values array into a StructArray with two entries
        let entries =
            StructArray::try_new(self.fields.clone(), vec![keys_array, values_array], None)
                .context(ArrowSnafu)?;
        let offsets = OffsetBuffer::from_lengths(lengths.into_iter().map(|l| l as usize));

        let field = Arc::new(Field::new_struct("entries", self.fields.clone(), false));
        let array =
            MapArray::try_new(field, offsets, entries, present, false).context(ArrowSnafu)?;
        let array = Arc::new(array);
        Ok(array)
    }
}
