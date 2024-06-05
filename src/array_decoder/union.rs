use std::sync::Arc;

use arrow::array::{ArrayRef, UnionArray};
use arrow::buffer::Buffer;
use arrow::datatypes::UnionFields;
use snafu::ResultExt;

use crate::column::{get_present_vec, Column};
use crate::error::ArrowSnafu;
use crate::error::Result;
use crate::proto::stream::Kind;
use crate::reader::decode::byte_rle::ByteRleIter;
use crate::stripe::Stripe;

use super::{array_decoder_factory, derive_present_vec, ArrayBatchDecoder};

/// Decode ORC Union column into batches of Arrow Sparse UnionArrays.
pub struct UnionArrayDecoder {
    // fields and variants should have same length
    // TODO: encode this assumption into types
    fields: UnionFields,
    variants: Vec<Box<dyn ArrayBatchDecoder>>,
    tags: Box<dyn Iterator<Item = Result<u8>> + Send>,
    present: Option<Box<dyn Iterator<Item = bool> + Send>>,
}

impl UnionArrayDecoder {
    pub fn new(column: &Column, fields: UnionFields, stripe: &Stripe) -> Result<Self> {
        let present = get_present_vec(column, stripe)?
            .map(|iter| Box::new(iter.into_iter()) as Box<dyn Iterator<Item = bool> + Send>);

        let tags = stripe.stream_map().get(column, Kind::Data);
        let tags = Box::new(ByteRleIter::new(tags));

        let variants = column
            .children()
            .iter()
            .zip(fields.iter())
            .map(|(child, (_id, field))| array_decoder_factory(child, field.clone(), stripe))
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            fields,
            variants,
            tags,
            present,
        })
    }
}

impl ArrayBatchDecoder for UnionArrayDecoder {
    fn next_batch(
        &mut self,
        batch_size: usize,
        parent_present: Option<&[bool]>,
    ) -> Result<ArrayRef> {
        let present = derive_present_vec(&mut self.present, parent_present, batch_size);
        let tags = match &present {
            Some(present) => {
                // Since UnionArrays don't have nullability, we rely on child arrays.
                // So we default to first child (tag 0) for any nulls from this parent Union.
                let mut tags = vec![0; batch_size];
                for index in present
                    .iter()
                    .enumerate()
                    .filter_map(|(index, &is_present)| is_present.then_some(index))
                {
                    // TODO: return as error instead
                    tags[index] = self
                        .tags
                        .next()
                        .transpose()?
                        .expect("array less than expected length");
                }
                tags
            }
            None => self
                .tags
                .by_ref()
                .take(batch_size)
                .collect::<Result<Vec<_>>>()?,
        };

        // Calculate nullability for children
        let mut children_nullability = (0..self.variants.len())
            .map(|index| {
                let mut child_present = vec![false; batch_size];
                for idx in tags
                    .iter()
                    .enumerate()
                    // Where the parent expects the value of the child, we set to non-null.
                    // Otherwise for the sparse spots, we leave as null in children.
                    .filter_map(|(idx, &tag)| (tag as usize == index).then_some(idx))
                {
                    child_present[idx] = true;
                }
                child_present
            })
            .collect::<Vec<_>>();
        // If parent says a slot is null, we need to ensure the first child (0-index) also
        // encodes this information, since as mentioned before, Arrow UnionArrays don't store
        // nullability and rely on their children. We default to first child to encode this
        // information so need to enforce that here.
        if let Some(present) = &present {
            for child_present in children_nullability[0]
                .iter_mut()
                .zip(present)
                .filter_map(|(child, &parent_present)| (!parent_present).then_some(child))
            {
                *child_present = false;
            }
        }

        let child_arrays = self
            .variants
            .iter_mut()
            .zip(children_nullability)
            .map(|(decoder, present)| decoder.next_batch(batch_size, Some(&present)))
            .collect::<Result<Vec<_>>>()?;

        // Currently default to decoding as Sparse UnionArray so no value offsets
        let field_type_ids = (0..self.variants.len() as i8).collect::<Vec<_>>();
        let type_ids = Buffer::from_vec(tags);
        let child_arrays = self
            .fields
            .iter()
            .map(|(_id, field)| field.as_ref().clone())
            .zip(child_arrays)
            .collect::<Vec<_>>();
        let array = UnionArray::try_new(&field_type_ids, type_ids, None, child_arrays)
            .context(ArrowSnafu)?;
        let array = Arc::new(array);
        Ok(array)
    }
}
