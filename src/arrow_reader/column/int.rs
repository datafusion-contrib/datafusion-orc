use snafu::OptionExt;

use crate::arrow_reader::column::present::new_present_iter;
use crate::arrow_reader::column::{Column, NullableIterator};
use crate::error::{InvalidColumnSnafu, Result};
use crate::proto::stream::Kind;
use crate::reader::decode::get_direct_signed_rle_reader;

pub fn new_i64_iter(column: &Column) -> Result<NullableIterator<i64>> {
    let present = new_present_iter(column)?.collect::<Result<Vec<_>>>()?;

    let iter = column
        .stream(Kind::Data)
        .transpose()?
        .map(|reader| get_direct_signed_rle_reader(column, reader))
        .context(InvalidColumnSnafu { name: &column.name })??;

    Ok(NullableIterator {
        present: Box::new(present.into_iter()),
        iter,
    })
}
