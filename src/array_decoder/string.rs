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

use std::io::Read;
use std::marker::PhantomData;
use std::sync::Arc;

use arrow::array::{ArrayRef, DictionaryArray, GenericByteArray, StringArray};
use arrow::buffer::{Buffer, NullBuffer, OffsetBuffer};
use arrow::compute::kernels::cast;
use arrow::datatypes::{ByteArrayType, DataType, GenericBinaryType, GenericStringType};
use snafu::ResultExt;

use crate::array_decoder::derive_present_vec;
use crate::column::Column;
use crate::compression::Decompressor;
use crate::encoding::integer::get_unsigned_rle_reader;
use crate::encoding::PrimitiveValueDecoder;
use crate::error::{ArrowSnafu, IoSnafu, Result};
use crate::proto::column_encoding::Kind as ColumnEncodingKind;
use crate::proto::stream::Kind;
use crate::stripe::Stripe;

use super::{ArrayBatchDecoder, Int64ArrayDecoder, PresentDecoder};

// TODO: reduce duplication with string below
pub fn new_binary_decoder(column: &Column, stripe: &Stripe) -> Result<Box<dyn ArrayBatchDecoder>> {
    let present = PresentDecoder::from_stripe(stripe, column);

    let lengths = stripe.stream_map().get(column, Kind::Length);
    let lengths = get_unsigned_rle_reader(column, lengths);

    let bytes = Box::new(stripe.stream_map().get(column, Kind::Data));
    Ok(Box::new(BinaryArrayDecoder::new(bytes, lengths, present)))
}

pub fn new_string_decoder(column: &Column, stripe: &Stripe) -> Result<Box<dyn ArrayBatchDecoder>> {
    let kind = column.encoding().kind();
    let present = PresentDecoder::from_stripe(stripe, column);

    let lengths = stripe.stream_map().get(column, Kind::Length);
    let lengths = get_unsigned_rle_reader(column, lengths);

    match kind {
        ColumnEncodingKind::Direct | ColumnEncodingKind::DirectV2 => {
            let bytes = Box::new(stripe.stream_map().get(column, Kind::Data));
            Ok(Box::new(DirectStringArrayDecoder::new(
                bytes, lengths, present,
            )))
        }
        ColumnEncodingKind::Dictionary | ColumnEncodingKind::DictionaryV2 => {
            let bytes = Box::new(stripe.stream_map().get(column, Kind::DictionaryData));
            // TODO: is this always guaranteed to be set for all dictionaries?
            let dictionary_size = column.dictionary_size();
            // We assume here we have fetched all the dictionary strings (according to size above)
            let dictionary_strings = DirectStringArrayDecoder::new(bytes, lengths, None)
                .next_byte_batch(dictionary_size, None)?;
            let dictionary_strings = Arc::new(dictionary_strings);

            let indexes = stripe.stream_map().get(column, Kind::Data);
            let indexes = get_unsigned_rle_reader(column, indexes);
            let indexes = Int64ArrayDecoder::new(indexes, present);

            Ok(Box::new(DictionaryStringArrayDecoder::new(
                indexes,
                dictionary_strings,
            )?))
        }
    }
}

// TODO: check this offset size type
pub type DirectStringArrayDecoder = GenericByteArrayDecoder<GenericStringType<i32>>;
pub type BinaryArrayDecoder = GenericByteArrayDecoder<GenericBinaryType<i32>>;

pub struct GenericByteArrayDecoder<T: ByteArrayType> {
    bytes: Box<Decompressor>,
    lengths: Box<dyn PrimitiveValueDecoder<i64> + Send>,
    present: Option<PresentDecoder>,
    phantom: PhantomData<T>,
}

impl<T: ByteArrayType> GenericByteArrayDecoder<T> {
    fn new(
        bytes: Box<Decompressor>,
        lengths: Box<dyn PrimitiveValueDecoder<i64> + Send>,
        present: Option<PresentDecoder>,
    ) -> Self {
        Self {
            bytes,
            lengths,
            present,
            phantom: Default::default(),
        }
    }

    fn next_byte_batch(
        &mut self,
        batch_size: usize,
        parent_present: Option<&NullBuffer>,
    ) -> Result<GenericByteArray<T>> {
        let present =
            derive_present_vec(&mut self.present, parent_present, batch_size).transpose()?;

        let mut lengths = vec![0; batch_size];
        if let Some(present) = &present {
            self.lengths.decode_spaced(&mut lengths, present)?;
        } else {
            self.lengths.decode(&mut lengths)?;
        }
        let total_length: i64 = lengths.iter().sum();
        // Fetch all data bytes at once
        let mut bytes = Vec::with_capacity(total_length as usize);
        self.bytes
            .by_ref()
            .take(total_length as u64)
            .read_to_end(&mut bytes)
            .context(IoSnafu)?;
        let bytes = Buffer::from(bytes);
        let offsets =
            OffsetBuffer::<T::Offset>::from_lengths(lengths.into_iter().map(|l| l as usize));

        let null_buffer = match present {
            // Edge case where keys of map cannot have a null buffer
            Some(present) if present.null_count() == 0 => None,
            _ => present,
        };
        let array =
            GenericByteArray::<T>::try_new(offsets, bytes, null_buffer).context(ArrowSnafu)?;
        Ok(array)
    }
}

impl<T: ByteArrayType> ArrayBatchDecoder for GenericByteArrayDecoder<T> {
    fn next_batch(
        &mut self,
        batch_size: usize,
        parent_present: Option<&NullBuffer>,
    ) -> Result<ArrayRef> {
        let array = self.next_byte_batch(batch_size, parent_present)?;
        let array = Arc::new(array) as ArrayRef;
        Ok(array)
    }
}

pub struct DictionaryStringArrayDecoder {
    indexes: Int64ArrayDecoder,
    dictionary: Arc<StringArray>,
}

impl DictionaryStringArrayDecoder {
    fn new(indexes: Int64ArrayDecoder, dictionary: Arc<StringArray>) -> Result<Self> {
        Ok(Self {
            indexes,
            dictionary,
        })
    }
}

impl ArrayBatchDecoder for DictionaryStringArrayDecoder {
    fn next_batch(
        &mut self,
        batch_size: usize,
        parent_present: Option<&NullBuffer>,
    ) -> Result<ArrayRef> {
        let keys = self
            .indexes
            .next_primitive_batch(batch_size, parent_present)?;
        // TODO: ORC spec states: For dictionary encodings the dictionary is sorted
        //       (in lexicographical order of bytes in the UTF-8 encodings).
        //       So we can set the is_ordered property here?
        let array = DictionaryArray::try_new(keys, self.dictionary.clone()).context(ArrowSnafu)?;
        // Cast back to StringArray to ensure all stripes have consistent datatype
        // TODO: Is there anyway to preserve the dictionary encoding?
        //       This costs performance.
        let array = cast(&array, &DataType::Utf8).context(ArrowSnafu)?;

        let array = Arc::new(array);
        Ok(array)
    }
}
