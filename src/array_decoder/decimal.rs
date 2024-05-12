use std::cmp::Ordering;

use crate::arrow_reader::column::get_present_vec;
use crate::error::Result;
use crate::proto::stream::Kind;
use crate::reader::decode::decimal::UnboundedVarintStreamDecoder;
use crate::stripe::Stripe;
use crate::{arrow_reader::column::Column, reader::decode::get_rle_reader};

use super::{ArrayBatchDecoder, DecimalArrayDecoder};

pub fn new_decimal_decoder(
    column: &Column,
    stripe: &Stripe,
    precision: u32,
    fixed_scale: u32,
) -> Result<Box<dyn ArrayBatchDecoder>> {
    let varint_iter = stripe.stream_map().get(column, Kind::Data);
    let varint_iter = Box::new(UnboundedVarintStreamDecoder::new(
        varint_iter,
        stripe.number_of_rows(),
    ));
    let varint_iter = varint_iter as Box<dyn Iterator<Item = Result<i128>> + Send>;

    // Scale is specified on a per varint basis (in addition to being encoded in the type)
    let scale_iter = stripe.stream_map().get(column, Kind::Secondary);
    let scale_iter = get_rle_reader::<i32, _>(column, scale_iter)?;

    let present = get_present_vec(column, stripe)?
        .map(|iter| Box::new(iter.into_iter()) as Box<dyn Iterator<Item = bool> + Send>);

    let iter = DecimalScaleRepairIter {
        varint_iter,
        scale_iter,
        fixed_scale,
    };
    let iter = Box::new(iter);

    Ok(Box::new(DecimalArrayDecoder::new(
        precision as u8,
        fixed_scale as i8,
        iter,
        present,
    )))
}

/// This iter fixes the scales of the varints decoded as scale is specified on a per
/// varint basis, and needs to align with type specified scale
struct DecimalScaleRepairIter {
    varint_iter: Box<dyn Iterator<Item = Result<i128>> + Send>,
    scale_iter: Box<dyn Iterator<Item = Result<i32>> + Send>,
    fixed_scale: u32,
}

impl DecimalScaleRepairIter {
    #[inline]
    fn next_helper(&mut self, varint: Result<i128>, scale: Result<i32>) -> Result<Option<i128>> {
        let varint = varint?;
        let scale = scale?;
        Ok(Some(fix_i128_scale(varint, self.fixed_scale, scale)))
    }
}

impl Iterator for DecimalScaleRepairIter {
    type Item = Result<i128>;

    fn next(&mut self) -> Option<Self::Item> {
        let varint = self.varint_iter.next()?;
        let scale = self.scale_iter.next()?;
        self.next_helper(varint, scale).transpose()
    }
}

fn fix_i128_scale(i: i128, fixed_scale: u32, varying_scale: i32) -> i128 {
    // TODO: Verify with C++ impl in ORC repo, which does this cast
    //       Not sure why scale stream can be signed if it gets casted to unsigned anyway
    //       https://github.com/apache/orc/blob/0014bec1e4cdd1206f5bae4f5c2000b9300c6eb1/c%2B%2B/src/ColumnReader.cc#L1459-L1476
    let varying_scale = varying_scale as u32;
    match fixed_scale.cmp(&varying_scale) {
        Ordering::Less => {
            // fixed_scale < varying_scale
            // Current scale of number is greater than scale of the array type
            // So need to divide to align the scale
            // TODO: this differs from C++ implementation, need to verify
            let scale_factor = varying_scale - fixed_scale;
            // TODO: replace with lookup table?
            let scale_factor = 10_i128.pow(scale_factor);
            i / scale_factor
        }
        Ordering::Equal => i,
        Ordering::Greater => {
            // fixed_scale > varying_scale
            // Current scale of number is smaller than scale of the array type
            // So need to multiply to align the scale
            // TODO: this differs from C++ implementation, need to verify
            let scale_factor = fixed_scale - varying_scale;
            // TODO: replace with lookup table?
            let scale_factor = 10_i128.pow(scale_factor);
            i * scale_factor
        }
    }
}
