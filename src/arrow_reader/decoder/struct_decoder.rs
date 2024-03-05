use crate::{
    arrow_reader::{
        column::{present::get_present_vec, Column},
        decoder::{array_decoder_factory, ArrayBatchDecoder},
        Stripe,
    },
    error::{ArrowSnafu, Result},
};
use std::sync::Arc;

use arrow::{
    array::{ArrayRef, StructArray},
    buffer::NullBuffer,
    datatypes::{Field, Fields},
};
use snafu::ResultExt;

use super::derive_present_vec;

pub struct StructArrayDecoder {
    fields: Fields,
    decoders: Vec<Box<dyn ArrayBatchDecoder>>,
    present: Option<Box<dyn Iterator<Item = bool> + Send>>,
}

impl StructArrayDecoder {
    pub fn new(column: &Column, stripe: &Stripe) -> Result<Self> {
        let present = get_present_vec(column, stripe)?
            .map(|iter| Box::new(iter.into_iter()) as Box<dyn Iterator<Item = bool> + Send>);

        let decoders = column
            .children()
            .iter()
            .map(|child| array_decoder_factory(child, stripe))
            .collect::<Result<Vec<_>>>()?;

        let fields = column
            .children()
            .into_iter()
            .map(Field::from)
            .map(Arc::new)
            .collect::<Vec<_>>();
        let fields = Fields::from(fields);

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
