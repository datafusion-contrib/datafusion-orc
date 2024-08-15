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

use crate::array_decoder::{derive_present_vec, populate_lengths_with_nulls};
use crate::column::{get_present_vec, Column};
use crate::error::{ArrowSnafu, Result};
use crate::proto::stream::Kind;
use crate::reader::decode::get_rle_reader;
use crate::stripe::Stripe;

use super::{array_decoder_factory, ArrayBatchDecoder};

pub struct MapArrayDecoder {
    keys: Box<dyn ArrayBatchDecoder>,
    values: Box<dyn ArrayBatchDecoder>,
    present: Option<Box<dyn Iterator<Item = bool> + Send>>,
    lengths: Box<dyn Iterator<Item = Result<u64>> + Send>,
    fields: Fields,
}

impl MapArrayDecoder {
    pub fn new(
        column: &Column,
        keys_field: Arc<Field>,
        values_field: Arc<Field>,
        stripe: &Stripe,
    ) -> Result<Self> {
        let present = get_present_vec(column, stripe)?
            .map(|iter| Box::new(iter.into_iter()) as Box<dyn Iterator<Item = bool> + Send>);

        let keys_column = &column.children()[0];
        let keys = array_decoder_factory(keys_column, keys_field.clone(), stripe)?;

        let values_column = &column.children()[1];
        let values = array_decoder_factory(values_column, values_field.clone(), stripe)?;

        let reader = stripe.stream_map().get(column, Kind::Length);
        let lengths = get_rle_reader(column, reader)?;

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
        parent_present: Option<&[bool]>,
    ) -> Result<ArrayRef> {
        let present = derive_present_vec(&mut self.present, parent_present, batch_size);

        // How many lengths we need to fetch
        let elements_to_fetch = if let Some(present) = &present {
            present.iter().filter(|&&is_present| is_present).count()
        } else {
            batch_size
        };
        let lengths = self
            .lengths
            .by_ref()
            .take(elements_to_fetch)
            .collect::<Result<Vec<_>>>()?;
        debug_assert_eq!(
            lengths.len(),
            elements_to_fetch,
            "less lengths than expected in MapArray"
        );
        let total_length: u64 = lengths.iter().sum();
        // Fetch key and value arrays, each with total_length elements
        // Fetch child array as one Array with total_length elements
        let keys_array = self.keys.next_batch(total_length as usize, None)?;
        let values_array = self.values.next_batch(total_length as usize, None)?;
        // Compose the keys + values array into a StructArray with two entries
        let entries =
            StructArray::try_new(self.fields.clone(), vec![keys_array, values_array], None)
                .context(ArrowSnafu)?;
        let lengths = populate_lengths_with_nulls(lengths, batch_size, &present);
        let offsets = OffsetBuffer::from_lengths(lengths);
        let null_buffer = present.map(NullBuffer::from);

        let field = Arc::new(Field::new_struct("entries", self.fields.clone(), false));
        let array =
            MapArray::try_new(field, offsets, entries, null_buffer, false).context(ArrowSnafu)?;
        let array = Arc::new(array);
        Ok(array)
    }
}
