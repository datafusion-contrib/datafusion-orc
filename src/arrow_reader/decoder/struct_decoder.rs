use crate::{
    arrow_reader::{
        column::{present::new_present_iter, Column},
        decoder::{array_decoder_factory, merge_parent_present, ArrayBatchDecoder},
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

pub struct StructArrayDecoder {
    fields: Fields,
    decoders: Vec<Box<dyn ArrayBatchDecoder>>,
    present: Box<dyn Iterator<Item = bool> + Send>,
}

impl StructArrayDecoder {
    pub fn new(column: &Column, stripe: &Stripe) -> Result<Self> {
        let present = new_present_iter(column, stripe)?.collect::<Result<Vec<_>>>()?;
        // TODO: this is to make it Send, fix this?
        let present = Box::new(present.into_iter());

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
    ) -> Result<Option<ArrayRef>> {
        let present = self.present.by_ref().take(batch_size);
        let present = if let Some(parent_present) = parent_present {
            debug_assert_eq!(
                parent_present.len(),
                batch_size,
                "when provided, parent_present length must equal batch_size"
            );
            merge_parent_present(parent_present, present)
        } else {
            present.collect::<Vec<_>>()
        };

        let child_arrays = self
            .decoders
            .iter_mut()
            .map(|child| child.next_batch(batch_size, Some(&present)))
            .collect::<Result<Vec<_>>>()?;

        // TODO: more strict, this should either be all Some or all None, not in-between
        //       throw error if in-between case
        if child_arrays.contains(&None) {
            Ok(None)
        } else {
            let child_arrays = child_arrays
                .into_iter()
                .map(|opt| opt.unwrap())
                .collect::<Vec<_>>();
            let null_buffer = NullBuffer::from(present);
            let array = StructArray::try_new(self.fields.clone(), child_arrays, Some(null_buffer))
                .context(ArrowSnafu)?;
            let array = Arc::new(array);
            Ok(Some(array))
        }
    }
}
