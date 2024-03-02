use crate::arrow_reader::column::present::new_present_iter;
use crate::arrow_reader::column::{Column, NullableIterator};
use crate::arrow_reader::Stripe;
use crate::error::Result;
use crate::proto::stream::Kind;
use crate::reader::decode::float::{Float, FloatIter};

pub fn new_float_iter<F: Float>(column: &Column, stripe: &Stripe) -> Result<NullableIterator<F>> {
    let present = new_present_iter(column, stripe)?.collect::<Result<Vec<_>>>()?;
    let rows = present.iter().filter(|&p| *p).count();
    let reader = stripe.stream_map.get(column, Kind::Data)?;
    let iter = Box::new(FloatIter::<F, _>::new(reader, rows));

    Ok(NullableIterator {
        present: Box::new(present.into_iter()),
        iter,
    })
}
