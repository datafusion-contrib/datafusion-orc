use std::sync::Arc;

use arrow::array::StringArray;
use snafu::{OptionExt, ResultExt};

use crate::arrow_reader::column::present::new_present_iter;
use crate::arrow_reader::column::{Column, NullableIterator};
use crate::arrow_reader::Stripe;
use crate::error::{self, Result};
use crate::proto::column_encoding::Kind as ColumnEncodingKind;
use crate::proto::stream::Kind;
use crate::reader::decode::variable_length::Values;
use crate::reader::decode::RleVersion;
use crate::reader::decompress::Decompressor;

pub struct DirectStringIterator {
    values: Box<Values<Decompressor>>,
    lengths: Box<dyn Iterator<Item = Result<u64>> + Send>,
}

impl Iterator for DirectStringIterator {
    type Item = Result<String>;
    fn next(&mut self) -> Option<Self::Item> {
        match self.lengths.next() {
            Some(Ok(length)) => match self.values.next(length as usize) {
                Ok(value) => Some(
                    std::str::from_utf8(value)
                        .map(|x| x.to_string())
                        .context(error::InvalidUft8Snafu),
                ),
                Err(err) => Some(Err(err)),
            },
            Some(Err(err)) => Some(Err(err)),
            None => None,
        }
    }
}

pub fn new_direct_string_iter(
    column: &Column,
    rle_version: RleVersion,
    stripe: &Stripe,
) -> Result<NullableIterator<String>> {
    let present = new_present_iter(column, stripe)?.collect::<Result<Vec<_>>>()?;

    let values = stripe
        .stream_map
        .get(column, Kind::Data)
        .map(|reader| Box::new(Values::new(reader, vec![])))
        .context(error::InvalidColumnSnafu { name: &column.name })?;

    let lengths = stripe
        .stream_map
        .get(column, Kind::Length)
        .map(|reader| rle_version.get_unsigned_rle_reader(reader))
        .context(error::InvalidColumnSnafu { name: &column.name })?;

    Ok(NullableIterator {
        present: Box::new(present.into_iter()),
        iter: Box::new(DirectStringIterator { values, lengths }),
    })
}

pub fn new_arrow_dict_string_decoder(
    column: &Column,
    rle_version: RleVersion,
    stripe: &Stripe,
) -> Result<(NullableIterator<u64>, Arc<StringArray>)> {
    let present = new_present_iter(column, stripe)?.collect::<Result<Vec<_>>>()?;

    // DictionaryData
    let values = stripe
        .stream_map
        .get(column, Kind::DictionaryData)
        .map(|reader| Box::new(Values::new(reader, vec![])))
        .context(error::InvalidColumnSnafu { name: &column.name })?;

    let lengths = stripe
        .stream_map
        .get(column, Kind::Length)
        .map(|reader| rle_version.get_unsigned_rle_reader(reader))
        .context(error::InvalidColumnSnafu { name: &column.name })?;
    let iter = DirectStringIterator { values, lengths };

    let values = iter.collect::<Result<Vec<_>>>()?;

    let indexes = stripe
        .stream_map
        .get(column, Kind::Data)
        .map(|reader| rle_version.get_unsigned_rle_reader(reader))
        .context(error::InvalidColumnSnafu { name: &column.name })?;

    let dictionary = StringArray::from_iter(values.into_iter().map(Some));

    Ok((
        NullableIterator {
            present: Box::new(present.into_iter()),
            iter: Box::new(indexes),
        },
        Arc::new(dictionary),
    ))
}

pub enum StringDecoder {
    Direct(NullableIterator<String>),
    Dictionary((NullableIterator<u64>, Arc<StringArray>)),
}

impl StringDecoder {
    pub fn new(column: &Column, stripe: &Stripe) -> Result<Self> {
        let kind = column.encoding().kind();

        match kind {
            ColumnEncodingKind::Direct | ColumnEncodingKind::DirectV2 => Ok(StringDecoder::Direct(
                new_direct_string_iter(column, kind.into(), stripe)?,
            )),
            ColumnEncodingKind::Dictionary | ColumnEncodingKind::DictionaryV2 => {
                Ok(StringDecoder::Dictionary(new_arrow_dict_string_decoder(
                    column,
                    kind.into(),
                    stripe,
                )?))
            }
        }
    }
}
