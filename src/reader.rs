use std::io::{Read, Seek};
use std::sync::Arc;

use snafu::OptionExt;

use self::column::Column;
use self::decompress::Decompressor;
use self::metadata::{read_metadata, read_stripe_footer, FileMetadata};
use self::schema::{create_schema, TypeDescription};
use crate::error::{self, Result};
use crate::proto::{StripeFooter, StripeInformation};

pub mod arrow;
mod column;
pub mod decode;
pub mod decompress;
pub mod metadata;
pub mod schema;

pub struct Reader<R: Read + Seek> {
    inner: R,
    metadata: Box<FileMetadata>,
    schema: Arc<TypeDescription>,
}

impl<R: Read + Seek> Reader<R> {
    pub fn new(mut r: R) -> Result<Self> {
        let metadata = Box::new(read_metadata(&mut r)?);
        let schema = create_schema(&metadata.footer.types, 0)?;

        Ok(Self {
            inner: r,
            metadata,
            schema,
        })
    }

    pub fn metadata(&self) -> &FileMetadata {
        &self.metadata
    }

    pub fn schema(&self) -> &TypeDescription {
        &self.schema
    }

    pub fn select(self, fields: &[&str]) -> Result<Cursor<R>> {
        Cursor::new(self, fields)
    }

    pub fn stripe(&self, index: usize) -> Option<StripeInformation> {
        self.metadata.footer.stripes.get(index).cloned()
    }

    pub fn stripe_footer(&mut self, stripe: usize) -> Result<StripeFooter> {
        read_stripe_footer(&mut self.inner, &self.metadata, stripe, &mut vec![])
    }
}

pub struct Cursor<R: Read + Seek> {
    reader: Reader<R>,
    columns: Vec<(String, Arc<TypeDescription>)>,
    stripe_offset: usize,
}

impl<R: Read + Seek> Cursor<R> {
    pub fn new(r: Reader<R>, fields: &[&str]) -> Result<Self> {
        let mut columns = Vec::with_capacity(fields.len());
        for &name in fields {
            let field = r
                .schema
                .field(name)
                .context(error::FieldNotFOundSnafu { name })?;
            columns.push((name.to_string(), field));
        }
        Ok(Self {
            reader: r,
            columns,
            stripe_offset: 0,
        })
    }
}

impl<R: Read + Seek> Iterator for Cursor<R> {
    type Item = Result<Stripe>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(info) = self.reader.stripe(self.stripe_offset) {
            let stripe = Stripe::new(&mut self.reader, &self.columns, self.stripe_offset, info);

            self.stripe_offset += 1;

            Some(stripe)
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub struct Stripe {
    footer: Arc<StripeFooter>,
    columns: Vec<Column>,
    stripe_offset: usize,
    info: StripeInformation,
}

impl Stripe {
    pub fn new<R: Read + Seek>(
        r: &mut Reader<R>,
        columns: &[(String, Arc<TypeDescription>)],
        stripe: usize,
        info: StripeInformation,
    ) -> Result<Self> {
        let footer = Arc::new(r.stripe_footer(stripe)?);

        let compression = r.metadata().postscript.compression();
        //TODO(weny): add tz
        let columns = columns
            .iter()
            .map(|(name, typ)| Column::new(r, compression, name, typ, &footer, &info))
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            footer,
            columns,
            stripe_offset: stripe,
            info,
        })
    }

    pub fn footer(&self) -> &Arc<StripeFooter> {
        &self.footer
    }

    pub fn stripe_offset(&self) -> usize {
        self.stripe_offset
    }

    pub fn stripe_info(&self) -> &StripeInformation {
        &self.info
    }
}
