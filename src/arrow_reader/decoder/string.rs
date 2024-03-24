use std::io::Read;
use std::marker::PhantomData;
use std::sync::Arc;

use arrow::array::{ArrayRef, DictionaryArray, GenericByteArray, StringArray};
use arrow::buffer::{Buffer, OffsetBuffer};
use arrow::compute::kernels::cast;
use arrow::datatypes::{ByteArrayType, DataType, GenericBinaryType, GenericStringType};
use snafu::ResultExt;

use crate::arrow_reader::column::{get_present_vec, Column};
use crate::arrow_reader::decoder::{
    create_null_buffer, derive_present_vec, populate_lengths_with_nulls, UInt64ArrayDecoder,
};
use crate::error::{ArrowSnafu, IoSnafu, Result};
use crate::proto::column_encoding::Kind as ColumnEncodingKind;
use crate::proto::stream::Kind;
use crate::reader::decode::{get_rle_reader, RleVersion};
use crate::reader::decompress::Decompressor;
use crate::stripe::Stripe;

use super::ArrayBatchDecoder;

// TODO: reduce duplication with string below
pub fn new_binary_decoder(column: &Column, stripe: &Stripe) -> Result<Box<dyn ArrayBatchDecoder>> {
    let present = get_present_vec(column, stripe)?
        .map(|iter| Box::new(iter.into_iter()) as Box<dyn Iterator<Item = bool> + Send>);

    let lengths = stripe.stream_map.get(column, Kind::Length);
    let lengths = get_rle_reader::<u64, _>(column, lengths)?;

    let bytes = Box::new(stripe.stream_map.get(column, Kind::Data));
    Ok(Box::new(BinaryArrayDecoder::new(bytes, lengths, present)))
}

pub fn new_string_decoder(column: &Column, stripe: &Stripe) -> Result<Box<dyn ArrayBatchDecoder>> {
    let kind = column.encoding().kind();
    let rle_version = RleVersion::from(kind);
    let present = get_present_vec(column, stripe)?
        .map(|iter| Box::new(iter.into_iter()) as Box<dyn Iterator<Item = bool> + Send>);

    let lengths = stripe.stream_map.get(column, Kind::Length);
    let lengths = rle_version.get_unsigned_rle_reader(lengths);

    match kind {
        ColumnEncodingKind::Direct | ColumnEncodingKind::DirectV2 => {
            let bytes = Box::new(stripe.stream_map.get(column, Kind::Data));
            Ok(Box::new(DirectStringArrayDecoder::new(
                bytes, lengths, present,
            )))
        }
        ColumnEncodingKind::Dictionary | ColumnEncodingKind::DictionaryV2 => {
            let bytes = Box::new(stripe.stream_map.get(column, Kind::DictionaryData));
            // TODO: is this always guaranteed to be set for all dictionaries?
            let dictionary_size = column.dictionary_size();
            // We assume here we have fetched all the dictionary strings (according to size above)
            let dictionary_strings = DirectStringArrayDecoder::new(bytes, lengths, None)
                .next_byte_batch(dictionary_size, None)?;
            let dictionary_strings = Arc::new(dictionary_strings);

            let indexes = stripe.stream_map.get(column, Kind::Data);
            let indexes = rle_version.get_unsigned_rle_reader(indexes);
            let indexes = UInt64ArrayDecoder::new(indexes, present);

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
    ) -> Result<GenericByteArray<T>> {
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
        let lengths = populate_lengths_with_nulls(lengths, batch_size, &present);
        let offsets = OffsetBuffer::<T::Offset>::from_lengths(lengths);
        let null_buffer = create_null_buffer(present);

        let array =
            GenericByteArray::<T>::try_new(offsets, bytes, null_buffer).context(ArrowSnafu)?;
        Ok(array)
    }
}

impl<T: ByteArrayType> ArrayBatchDecoder for GenericByteArrayDecoder<T> {
    fn next_batch(
        &mut self,
        batch_size: usize,
        parent_present: Option<&[bool]>,
    ) -> Result<ArrayRef> {
        let array = self.next_byte_batch(batch_size, parent_present)?;
        let array = Arc::new(array) as ArrayRef;
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
