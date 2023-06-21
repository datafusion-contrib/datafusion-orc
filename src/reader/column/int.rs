use snafu::OptionExt;

use super::GenericIterator;
use crate::error::{InvalidColumnSnafu, Result};
use crate::proto::stream::Kind;
use crate::reader::column::present::new_present_iter;
use crate::reader::column::{Column, NullableIterator};
use crate::reader::decode::rle_v2::SignedRleV2Iter;

macro_rules! impl_integer_iter {
    ($iter:ident,$tp:ident) => {
        paste::item! {
            pub fn [<new_ $tp _iter>] (column: &Column) -> Result<GenericIterator<$tp>> {
                let present = new_present_iter(column)?.try_collect::<Vec<_>>()?;
                let rows: usize = present.iter().filter(|&p| *p).count();

                let iter = column
                    .stream(Kind::Data)
                    .transpose()?
                    .map(|reader| {
                        Box::new($iter::new(reader, rows, vec![]))
                            as Box<dyn Iterator<Item = Result<$tp>>>
                    })
                    .context(InvalidColumnSnafu { name: &column.name })?;

                Ok(NullableIterator {
                    present: Box::new(present.into_iter()),
                    iter,
                })
            }
        }
    };
}

impl_integer_iter!(SignedRleV2Iter, i64);
