use snafu::OptionExt;

use crate::error::{InvalidColumnSnafu, Result};
use crate::proto::stream::Kind;
use crate::reader::column::present::new_present_iter;
use crate::reader::column::{Column, NullableIterator};
use crate::reader::decode::float::FloatIter;

macro_rules! impl_float_iter {
    ($tp:ident) => {
        paste::item! {
            pub fn [<new_ $tp _iter>] (column: &Column) -> Result<NullableIterator<$tp>> {
                let present = new_present_iter(column)?.try_collect::<Vec<_>>()?;
                let rows: usize = present.iter().filter(|&p| *p).count();
                let iter = column
                    .stream(Kind::Data)
                    .transpose()?
                    .map(|reader| Box::new(FloatIter::<$tp, _>::new(reader, rows)))
                    .context(InvalidColumnSnafu { name: &column.name })?;

                Ok(NullableIterator {
                    present: Box::new(present.into_iter()),
                    iter,
                })
            }
        }
    };
}

impl_float_iter!(f32);
impl_float_iter!(f64);
