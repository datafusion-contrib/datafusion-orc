// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::arrow_reader::column::present::new_present_iter;
use crate::arrow_reader::column::{Column, NullableIterator};
use crate::arrow_reader::Stripe;
use crate::error::Result;
use crate::proto::stream::Kind;
use crate::reader::decode::get_rle_reader;
use crate::reader::decode::variable_length::Values;
use crate::reader::decompress::Decompressor;

pub fn new_binary_iterator(column: &Column, stripe: &Stripe) -> Result<NullableIterator<Vec<u8>>> {
    let null_mask = new_present_iter(column, stripe)?.collect::<Result<Vec<_>>>()?;

    let values = stripe
        .stream_map
        .get(column, Kind::Data)
        .map(|reader| Box::new(Values::new(reader, vec![])))?;

    let lengths = stripe.stream_map.get(column, Kind::Length)?;
    let lengths = get_rle_reader(column, lengths)?;

    Ok(NullableIterator {
        present: Box::new(null_mask.into_iter()),
        iter: Box::new(DirectBinaryIterator { values, lengths }),
    })
}

pub struct DirectBinaryIterator {
    values: Box<Values<Decompressor>>,
    lengths: Box<dyn Iterator<Item = Result<u64>> + Send>,
}

impl DirectBinaryIterator {
    fn iter_next(&mut self) -> Result<Option<Vec<u8>>> {
        let next = match self.lengths.next() {
            Some(length) => Some(self.values.next(length? as usize)?.to_vec()),
            None => None,
        };
        Ok(next)
    }
}

impl Iterator for DirectBinaryIterator {
    type Item = Result<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter_next().transpose()
    }
}
