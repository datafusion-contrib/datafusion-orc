use crate::{
    arrow_reader::{reader_factory, Decoder, Stripe},
    error::Result,
};
use std::sync::Arc;

use arrow::{
    array::{ArrayBuilder, ArrayRef, StructBuilder},
    datatypes::{Field, Fields},
};

use super::{present::new_present_iter, Column};

pub struct StructDecoder {
    pub(crate) fields: Fields,
    pub(crate) decoders: Vec<Decoder>,
    present: Box<dyn Iterator<Item = bool> + Send>,
}

impl StructDecoder {
    pub fn collect_chunk(
        &mut self,
        root_builder: &mut StructBuilder,
        chunk: usize,
    ) -> Option<Result<Vec<ArrayRef>>> {
        let mut present = Vec::with_capacity(chunk);

        for _ in 0..chunk {
            match self.present.next() {
                Some(value) => {
                    present.push(value);
                }
                None => break,
            }
        }

        let mut builders = Vec::with_capacity(self.decoders.len());
        for decoder in &self.decoders {
            let builder = decoder.new_array_builder(1);
            builders.push(builder);
        }

        if present.is_empty() {
            return None;
        }

        for present in present {
            root_builder.append(present);
            if present {
                for (idx, decoder) in &mut self.decoders.iter_mut().enumerate() {
                    if let Err(err) = decoder.append_value(&mut builders[idx], 1) {
                        return Some(Err(err));
                    }
                }
            } else {
                for (idx, decoder) in &mut self.decoders.iter_mut().enumerate() {
                    if let Err(err) = decoder.append_null(&mut builders[idx]) {
                        return Some(Err(err));
                    }
                }
            }
        }

        let output = builders
            .into_iter()
            .map(|mut b| b.finish())
            .collect::<Vec<_>>();

        Some(Ok(output))
    }
}

impl StructDecoder {
    pub fn new_builder(&self, capacity: usize) -> Box<dyn ArrayBuilder> {
        let mut builders = Vec::with_capacity(self.decoders.len());
        for decoder in &self.decoders {
            let builder = decoder.new_array_builder(capacity);
            builders.push(builder);
        }

        Box::new(StructBuilder::new(self.fields.clone(), builders))
    }
}

pub fn new_struct_iter(column: &Column, stripe: &Stripe) -> Result<StructDecoder> {
    let present = new_present_iter(column, stripe)?.collect::<Result<Vec<_>>>()?;

    let children = column.children();
    let mut decoders = Vec::with_capacity(children.len());
    for column in &children {
        decoders.push(reader_factory(column, stripe)?);
    }

    let fields = children
        .into_iter()
        .map(Field::from)
        .map(Arc::new)
        .collect::<Vec<_>>();

    Ok(StructDecoder {
        fields: Fields::from_iter(fields),
        decoders,
        present: Box::new(present.into_iter()),
    })
}
