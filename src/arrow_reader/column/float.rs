use crate::arrow_reader::column::present::new_present_iter;
use crate::arrow_reader::column::{Column, NullableIterator};
use crate::arrow_reader::Stripe;
use crate::error::Result;
use crate::proto::stream::Kind;
use crate::reader::decode::float::FloatIter;

macro_rules! impl_float_iter {
    ($tp:ident) => {
        paste::item! {
            pub fn [<new_ $tp _iter>] (column: &Column, stripe: &Stripe) -> Result<NullableIterator<$tp>> {
                let present = new_present_iter(column, stripe)?.collect::<Result<Vec<_>>>()?;
                let rows: usize = present.iter().filter(|&p| *p).count();
                let reader = stripe.stream_map.get(column, Kind::Data)?;
                let iter = Box::new(FloatIter::<$tp, _>::new(reader, rows));

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
