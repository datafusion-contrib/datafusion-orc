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

use snafu::OptionExt;

use crate::arrow_reader::column::present::new_present_iter;
use crate::arrow_reader::column::{Column, NullableIterator};
use crate::error;
use crate::proto::stream::Kind;
use crate::reader::decode::rle_v2::RleReaderV2;
use crate::reader::decode::variable_length::Values;
use crate::reader::decompress::Decompressor;

pub fn new_binary_iterator(column: &Column) -> error::Result<NullableIterator<Vec<u8>>> {
    let null_mask = new_present_iter(column)?.try_collect::<Vec<_>>()?;

    let values = column
        .stream(Kind::Data)
        .transpose()?
        .map(|reader| Box::new(Values::new(reader, vec![])))
        .context(error::InvalidColumnSnafu { name: &column.name })?;
    let lengths = column
        .stream(Kind::Length)
        .transpose()?
        .map(|reader| Box::new(RleReaderV2::try_new(reader, false, true)))
        .context(error::InvalidColumnSnafu { name: &column.name })?;

    Ok(NullableIterator {
        present: Box::new(null_mask.into_iter()),
        iter: Box::new(DirectBinaryIterator { values, lengths }),
    })
}

pub struct DirectBinaryIterator {
    values: Box<Values<Decompressor>>,
    lengths: Box<dyn Iterator<Item = error::Result<i64>> + Send>,
}

impl Iterator for DirectBinaryIterator {
    type Item = error::Result<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.lengths.next() {
            Some(Ok(length)) => match self.values.next(length as usize) {
                Ok(value) => Some(Ok(value.to_vec())),
                Err(err) => Some(Err(err)),
            },
            Some(Err(err)) => Some(Err(err)),
            None => None,
        }
    }
}
