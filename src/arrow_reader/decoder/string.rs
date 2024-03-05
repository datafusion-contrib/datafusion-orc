use std::io::Read;
use std::marker::PhantomData;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, DictionaryArray, GenericByteArray, StringArray};
use arrow::buffer::{Buffer, NullBuffer, OffsetBuffer};
use arrow::datatypes::{ByteArrayType, GenericBinaryType, GenericStringType};
use snafu::ResultExt;

use crate::arrow_reader::column::present::new_present_iter;
use crate::arrow_reader::column::{Column, NullableIterator};
use crate::arrow_reader::decoder::{merge_parent_present, UInt64ArrayDecoder};
use crate::error::{ArrowSnafu, IoSnafu, Result};
use crate::proto::column_encoding::Kind as ColumnEncodingKind;
use crate::proto::stream::Kind;
use crate::reader::decode::{get_rle_reader, RleVersion};
use crate::reader::decompress::Decompressor;
use crate::stripe::Stripe;

use super::ArrayBatchDecoder;

// TODO: reduce duplication with string below
pub fn new_binary_decoder(column: &Column, stripe: &Stripe) -> Result<Box<dyn ArrayBatchDecoder>> {
    let present = new_present_iter(column, stripe)?.collect::<Result<Vec<_>>>()?;
    // TODO: this is to make it Send, fix this?
    let present = Box::new(present.into_iter());

    let lengths = stripe.stream_map.get(column, Kind::Length)?;
    let lengths = get_rle_reader::<u64, _>(column, lengths)?;

    let bytes = Box::new(stripe.stream_map.get(column, Kind::Data)?);
    Ok(Box::new(BinaryArrayDecoder::new(
        bytes,
        lengths,
        Some(present),
    )))
}

pub fn new_string_decoder(column: &Column, stripe: &Stripe) -> Result<Box<dyn ArrayBatchDecoder>> {
    let kind = column.encoding().kind();
    let rle_version = RleVersion::from(kind);
    let present = new_present_iter(column, stripe)?.collect::<Result<Vec<_>>>()?;
    // TODO: this is to make it Send, fix this?
    let present = Box::new(present.into_iter());

    let lengths = stripe.stream_map.get(column, Kind::Length)?;
    let lengths = rle_version.get_unsigned_rle_reader(lengths);

    match kind {
        ColumnEncodingKind::Direct | ColumnEncodingKind::DirectV2 => {
            let bytes = Box::new(stripe.stream_map.get(column, Kind::Data)?);
            Ok(Box::new(DirectStringArrayDecoder::new(
                bytes,
                lengths,
                Some(present),
            )))
        }
        ColumnEncodingKind::Dictionary | ColumnEncodingKind::DictionaryV2 => {
            let bytes = Box::new(stripe.stream_map.get(column, Kind::DictionaryData)?);
            // TODO: is this always guaranteed to be set for all dictionaries?
            let dictionary_size = column.dictionary_size();
            debug_assert!(dictionary_size > 0, "dictionary cannot be empty");
            // We assume here we have fetched all the dictionary strings (according to size above)
            let dictionary_strings = DirectStringArrayDecoder::new(bytes, lengths, None)
                .next_byte_batch(dictionary_size, None)?
                .unwrap();
            let dictionary_strings = Arc::new(dictionary_strings);

            let indexes = stripe.stream_map.get(column, Kind::Data)?;
            let indexes = rle_version.get_unsigned_rle_reader(indexes);
            let indexes = NullableIterator {
                present: Box::new(present.into_iter()),
                iter: Box::new(indexes),
            };
            let indexes = UInt64ArrayDecoder::new(indexes);

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
    lengths: Box<dyn Iterator<Item = Result<u64>> + Send>,
    present: Option<Box<dyn Iterator<Item = bool> + Send>>,
    phantom: PhantomData<T>,
}

impl<T: ByteArrayType> GenericByteArrayDecoder<T> {
    fn new(
        bytes: Box<Decompressor>,
        lengths: Box<dyn Iterator<Item = Result<u64>> + Send>,
        present: Option<Box<dyn Iterator<Item = bool> + Send>>,
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
        parent_present: Option<&[bool]>,
    ) -> Result<Option<GenericByteArray<T>>> {
        let present = match (&mut self.present, parent_present) {
            (Some(present), Some(parent_present)) => {
                let present = present.by_ref().take(batch_size);
                Some(merge_parent_present(parent_present, present))
            }
            (Some(present), None) => Some(present.by_ref().take(batch_size).collect::<Vec<_>>()),
            (None, Some(parent_present)) => Some(parent_present.to_vec()),
            (None, None) => None,
        };
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
            "less lengths than expected in ByteArray"
        );
        let total_length: u64 = lengths.iter().sum();
        // Fetch all data bytes at once
        let mut bytes = Vec::with_capacity(total_length as usize);
        self.bytes
            .by_ref()
            .take(total_length)
            .read_to_end(&mut bytes)
            .context(IoSnafu)?;
        let bytes = Buffer::from(bytes);
        // Fix the lengths to account for nulls (represented as 0 length)
        let lengths = if let Some(present) = &present {
            let mut lengths_with_nulls = Vec::with_capacity(batch_size);
            let mut lengths = lengths.iter();
            for &is_present in present {
                if is_present {
                    let length = *lengths.next().unwrap();
                    lengths_with_nulls.push(length as usize);
                } else {
                    lengths_with_nulls.push(0);
                }
            }
            lengths_with_nulls
        } else {
            lengths.into_iter().map(|l| l as usize).collect()
        };
        let offsets = OffsetBuffer::<T::Offset>::from_lengths(lengths);
        let null_buffer = match present {
            // Edge case where keys of map cannot have a null buffer
            Some(present) if present.iter().all(|&p| p) => None,
            Some(present) => Some(NullBuffer::from(present)),
            None => None,
        };

        let array =
            GenericByteArray::<T>::try_new(offsets, bytes, null_buffer).context(ArrowSnafu)?;
        if array.is_empty() {
            Ok(None)
        } else {
            Ok(Some(array))
        }
    }
}

impl<T: ByteArrayType> ArrayBatchDecoder for GenericByteArrayDecoder<T> {
    fn next_batch(
        &mut self,
        batch_size: usize,
        parent_present: Option<&[bool]>,
    ) -> Result<Option<ArrayRef>> {
        let array = self.next_byte_batch(batch_size, parent_present)?;
        let array = array.map(|a| Arc::new(a) as ArrayRef);
        Ok(array)
    }
}

pub struct DictionaryStringArrayDecoder {
    indexes: UInt64ArrayDecoder,
    dictionary: Arc<StringArray>,
}

impl DictionaryStringArrayDecoder {
    fn new(indexes: UInt64ArrayDecoder, dictionary: Arc<StringArray>) -> Result<Self> {
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
        parent_present: Option<&[bool]>,
    ) -> Result<Option<ArrayRef>> {
        let keys = self
            .indexes
            .next_primitive_batch(batch_size, parent_present)?
            .unwrap();
        let array = DictionaryArray::try_new(keys, self.dictionary.clone()).context(ArrowSnafu)?;

        let array = Arc::new(array);
        if array.is_empty() {
            Ok(None)
        } else {
            Ok(Some(array))
        }
    }
}
