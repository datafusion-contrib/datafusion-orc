use snafu::OptionExt;

use crate::arrow_reader::column::present::new_present_iter;
use crate::arrow_reader::column::{Column, NullableIterator};
use crate::error::{InvalidColumnSnafu, Result};
use crate::proto::stream::Kind;
use crate::reader::decode::byte_rle::ByteRleIter;

pub fn new_u8_iter(column: &Column) -> Result<NullableIterator<u8>> {
    let present = new_present_iter(column)?.collect::<Result<Vec<_>>>()?;
    let rows: usize = present.iter().filter(|&p| *p).count();

    let iter = column
        .stream(Kind::Data)
        .transpose()?
        .map(|reader| Box::new(ByteRleIter::new(reader, rows)) as _)
        .context(InvalidColumnSnafu { name: &column.name })?;

    Ok(NullableIterator {
        present: Box::new(present.into_iter()),
        iter,
    })
}
