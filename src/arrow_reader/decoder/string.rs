use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringArray, StringBuilder, StringDictionaryBuilder};
use arrow::datatypes::UInt64Type;
use snafu::ResultExt;

use crate::arrow_reader::column::present::new_present_iter;
use crate::arrow_reader::column::{Column, NullableIterator};
use crate::error::{InvalidUft8Snafu, Result};
use crate::proto::column_encoding::Kind as ColumnEncodingKind;
use crate::proto::stream::Kind;
use crate::reader::decode::variable_length::Values;
use crate::reader::decode::RleVersion;
use crate::reader::decompress::Decompressor;
use crate::stripe::Stripe;

use super::ArrayBatchDecoder;

pub struct DirectStringIterator {
    values: Box<Values<Decompressor>>,
    lengths: Box<dyn Iterator<Item = Result<u64>> + Send>,
}

impl DirectStringIterator {
    fn iter_next(&mut self) -> Result<Option<String>> {
        let next = match self.lengths.next() {
            Some(length) => {
                let value = self.values.next(length? as usize)?;
                Some(String::from_utf8(value.to_vec()).context(InvalidUft8Snafu)?)
            }
            None => None,
        };
        Ok(next)
    }
}

impl Iterator for DirectStringIterator {
    type Item = Result<String>;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter_next().transpose()
    }
}

fn new_direct_string_iter(
    column: &Column,
    rle_version: RleVersion,
    stripe: &Stripe,
) -> Result<NullableIterator<String>> {
    let present = new_present_iter(column, stripe)?.collect::<Result<Vec<_>>>()?;

    let reader = stripe.stream_map.get(column, Kind::Data)?;
    let values = Box::new(Values::new(reader, vec![]));

    let reader = stripe.stream_map.get(column, Kind::Length)?;
    let lengths = rle_version.get_unsigned_rle_reader(reader);

    Ok(NullableIterator {
        present: Box::new(present.into_iter()),
        iter: Box::new(DirectStringIterator { values, lengths }),
    })
}

fn new_arrow_dict_string_decoder(
    column: &Column,
    rle_version: RleVersion,
    stripe: &Stripe,
) -> Result<(NullableIterator<u64>, Arc<StringArray>)> {
    let present = new_present_iter(column, stripe)?.collect::<Result<Vec<_>>>()?;

    // DictionaryData
    let reader = stripe.stream_map.get(column, Kind::DictionaryData)?;
    let values = Box::new(Values::new(reader, vec![]));

    let reader = stripe.stream_map.get(column, Kind::Length)?;
    let lengths = rle_version.get_unsigned_rle_reader(reader);

    let iter = DirectStringIterator { values, lengths };

    let values = iter.collect::<Result<Vec<_>>>()?;

    let reader = stripe.stream_map.get(column, Kind::Data)?;
    let indexes = rle_version.get_unsigned_rle_reader(reader);

    let dictionary = StringArray::from_iter(values.into_iter().map(Some));

    Ok((
        NullableIterator {
            present: Box::new(present.into_iter()),
            iter: Box::new(indexes),
        },
        Arc::new(dictionary),
    ))
}

pub fn new_string_decoder(column: &Column, stripe: &Stripe) -> Result<Box<dyn ArrayBatchDecoder>> {
    let kind = column.encoding().kind();

    match kind {
        ColumnEncodingKind::Direct | ColumnEncodingKind::DirectV2 => {
            let inner = new_direct_string_iter(column, kind.into(), stripe)?;
            Ok(Box::new(DirectStringArrayDecoder::new(inner)))
        }
        ColumnEncodingKind::Dictionary | ColumnEncodingKind::DictionaryV2 => {
            let (indices, dictionary) = new_arrow_dict_string_decoder(column, kind.into(), stripe)?;
            Ok(Box::new(DictionaryStringArrayDecoder::new(
                indices, dictionary,
            )))
        }
    }
}

pub struct DirectStringArrayDecoder {
    inner: NullableIterator<String>,
}

impl DirectStringArrayDecoder {
    pub fn new(inner: NullableIterator<String>) -> Self {
        Self { inner }
    }
}

impl ArrayBatchDecoder for DirectStringArrayDecoder {
    fn next_batch(
        &mut self,
        batch_size: usize,
        parent_present: Option<&[bool]>,
    ) -> Result<Option<ArrayRef>> {
        let mut builder = StringBuilder::new();

        let mut iter = self.inner.by_ref().take(batch_size);
        if let Some(parent_present) = parent_present {
            debug_assert_eq!(
                parent_present.len(),
                batch_size,
                "when provided, parent_present length must equal batch_size"
            );

            for &is_present in parent_present {
                if is_present {
                    // TODO: return as error instead
                    let opt = iter
                        .next()
                        .transpose()?
                        .expect("array less than expected length");
                    builder.append_option(opt);
                } else {
                    builder.append_null();
                }
            }
        } else {
            for opt in iter {
                let opt = opt?;
                builder.append_option(opt);
            }
        };

        let array = Arc::new(builder.finish());
        if array.is_empty() {
            Ok(None)
        } else {
            Ok(Some(array))
        }
    }
}

pub struct DictionaryStringArrayDecoder {
    indexes: NullableIterator<u64>,
    dictionary: Arc<StringArray>,
}

impl DictionaryStringArrayDecoder {
    pub fn new(indexes: NullableIterator<u64>, dictionary: Arc<StringArray>) -> Self {
        Self {
            indexes,
            dictionary,
        }
    }
}

impl ArrayBatchDecoder for DictionaryStringArrayDecoder {
    fn next_batch(
        &mut self,
        batch_size: usize,
        parent_present: Option<&[bool]>,
    ) -> Result<Option<ArrayRef>> {
        // Safety: keys won't overflow
        let mut builder = StringDictionaryBuilder::<UInt64Type>::new_with_dictionary(
            batch_size,
            &self.dictionary,
        )
        .unwrap();

        let mut indexes = self.indexes.by_ref().take(batch_size);
        if let Some(parent_present) = parent_present {
            debug_assert_eq!(
                parent_present.len(),
                batch_size,
                "when provided, parent_present length must equal batch_size"
            );

            for &is_present in parent_present {
                if is_present {
                    // TODO: return as error instead
                    let index = indexes
                        .next()
                        .transpose()?
                        .expect("array less than expected length");
                    builder.append_option(index.map(|idx| self.dictionary.value(idx as usize)));
                } else {
                    builder.append_null();
                }
            }
        } else {
            for index in indexes {
                let index = index?;
                builder.append_option(index.map(|idx| self.dictionary.value(idx as usize)));
            }
        };

        let array = Arc::new(builder.finish());
        if array.is_empty() {
            Ok(None)
        } else {
            Ok(Some(array))
        }
    }
}
