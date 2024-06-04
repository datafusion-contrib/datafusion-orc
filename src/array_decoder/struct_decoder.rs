use std::sync::Arc;

use arrow::{
    array::{ArrayRef, StructArray},
    buffer::NullBuffer,
    datatypes::Fields,
};
use snafu::ResultExt;

use crate::stripe::Stripe;
use crate::{column::get_present_vec, error::Result};
use crate::{column::Column, error::ArrowSnafu};

use super::{array_decoder_factory, derive_present_vec, ArrayBatchDecoder};

pub struct StructArrayDecoder {
    fields: Fields,
    decoders: Vec<Box<dyn ArrayBatchDecoder>>,
    present: Option<Box<dyn Iterator<Item = bool> + Send>>,
}

impl StructArrayDecoder {
    pub fn new(column: &Column, fields: Fields, stripe: &Stripe) -> Result<Self> {
        println!("StructArrayDecoder column: {:#?}", column);
        println!("StructArrayDecoder fields: {:#?}", fields);
        let present = get_present_vec(column, stripe)?
            .map(|iter| Box::new(iter.into_iter()) as Box<dyn Iterator<Item = bool> + Send>);

        let decoders = column
            .children()
            .iter()
            .zip(fields.iter().cloned())
            .map(|(child, field)| array_decoder_factory(child, field, stripe))
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            decoders,
            present,
            fields,
        })
    }
}

impl ArrayBatchDecoder for StructArrayDecoder {
    fn next_batch(
        &mut self,
        batch_size: usize,
        parent_present: Option<&[bool]>,
    ) -> Result<ArrayRef> {
        let present = derive_present_vec(&mut self.present, parent_present, batch_size);

        let child_arrays = self
            .decoders
            .iter_mut()
            .map(|child| child.next_batch(batch_size, present.as_deref()))
            .collect::<Result<Vec<_>>>()?;

        let null_buffer = present.map(NullBuffer::from);
        let array = StructArray::try_new(self.fields.clone(), child_arrays, null_buffer)
            .context(ArrowSnafu)?;
        let array = Arc::new(array);
        Ok(array)
    }
}
