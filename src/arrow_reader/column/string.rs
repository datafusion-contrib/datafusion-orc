use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray};
use snafu::{OptionExt, ResultExt};

use crate::arrow_reader::column::present::new_present_iter;
use crate::arrow_reader::column::{Column, NullableIterator};
use crate::error::{self, Result};
use crate::proto::column_encoding::Kind as ColumnEncodingKind;
use crate::proto::stream::Kind;
use crate::reader::decode::rle_v2::RleReaderV2;
use crate::reader::decode::variable_length::Values;
use crate::reader::decompress::Decompressor;

pub struct DirectStringIterator {
    values: Box<Values<Decompressor>>,
    lengths: Box<dyn Iterator<Item = Result<i64>> + Send>,
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

pub fn new_direct_string_iter(column: &Column) -> Result<NullableIterator<String>> {
    let present = new_present_iter(column)?.try_collect::<Vec<_>>()?;

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
        present: Box::new(present.into_iter()),
        iter: Box::new(DirectStringIterator { values, lengths }),
    })
}

pub fn new_arrow_dict_string_decoder(column: &Column) -> Result<(NullableIterator<i64>, ArrayRef)> {
    let present = new_present_iter(column)?.try_collect::<Vec<_>>()?;

    // DictionaryData
    let values = column
        .stream(Kind::DictionaryData)
        .transpose()?
        .map(|reader| Box::new(Values::new(reader, vec![])))
        .context(error::InvalidColumnSnafu { name: &column.name })?;

    let lengths = column
        .stream(Kind::Length)
        .transpose()?
        .map(|reader| Box::new(RleReaderV2::try_new(reader, false, true)))
        .context(error::InvalidColumnSnafu { name: &column.name })?;
    let mut iter = DirectStringIterator { values, lengths };

    let values = iter.try_collect::<Vec<_>>()?;

    let indexes = column
        .stream(Kind::Data)
        .transpose()?
        .map(|reader| Box::new(RleReaderV2::try_new(reader, false, true)))
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
    Dictionary((NullableIterator<i64>, ArrayRef)),
}

impl StringDecoder {
    pub fn new(column: &Column) -> Result<Self> {
        match column.encoding().kind() {
            ColumnEncodingKind::DirectV2 => {
                Ok(StringDecoder::Direct(new_direct_string_iter(column)?))
            }
            ColumnEncodingKind::DictionaryV2 => Ok(StringDecoder::Dictionary(
                new_arrow_dict_string_decoder(column)?,
            )),
            other => unimplemented!("{other:?}"),
        }
    }
}
