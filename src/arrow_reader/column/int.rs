use snafu::OptionExt;

use crate::arrow_reader::column::present::new_present_iter;
use crate::arrow_reader::column::{Column, NullableIterator};
use crate::error::{InvalidColumnSnafu, Result};
use crate::proto::stream::Kind;
use crate::reader::decode::rle_v2::RleReaderV2;

pub fn new_i64_iter(column: &Column) -> Result<NullableIterator<i64>> {
    let present = new_present_iter(column)?.collect::<Result<Vec<_>>>()?;

    let iter = column
        .stream(Kind::Data)
        .transpose()?
        .map(|reader| {
            Box::new(RleReaderV2::try_new(reader, true, true))
                as Box<dyn Iterator<Item = Result<i64>> + Send>
        })
        .context(InvalidColumnSnafu { name: &column.name })?;

    Ok(NullableIterator {
        present: Box::new(present.into_iter()),
        iter,
    })
}
