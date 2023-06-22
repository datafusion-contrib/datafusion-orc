use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

use bytes::Bytes;
use snafu::{OptionExt, ResultExt};

use super::schema::TypeDescription;
use super::Reader;
use crate::error::{self, Result};
use crate::proto::stream::Kind;
use crate::proto::{ColumnEncoding, CompressionKind, StripeFooter, StripeInformation};
use crate::reader::Decompressor;

pub mod boolean;
pub mod date;
pub mod float;
pub mod int;
pub mod present;
pub mod string;
pub mod timestamp;

#[derive(Debug)]
pub struct Column {
    data: Bytes,
    number_of_rows: u64,
    compression: CompressionKind,
    footer: Arc<StripeFooter>,
    name: String,
    column: Arc<TypeDescription>,
}

impl Column {
    pub fn new<R: Read + Seek>(
        reader: &mut Reader<R>,
        compression: CompressionKind,
        name: &str,
        column: &Arc<TypeDescription>,
        footer: &Arc<StripeFooter>,
        stripe: &StripeInformation,
    ) -> Result<Self> {
        let mut start = 0; // the start of the stream

        let column_idx = column.column_id() as u32;

        let start = footer
            .streams
            .iter()
            .map(|stream| {
                start += stream.length();
                (start, stream)
            })
            .find(|(_, stream)| stream.column() == column_idx && stream.kind() != Kind::RowIndex)
            .map(|(start, stream)| start - stream.length())
            .with_context(|| error::InvalidColumnSnafu { name })?;

        let length = footer
            .streams
            .iter()
            .filter(|stream| stream.column() == column_idx && stream.kind() != Kind::RowIndex)
            .fold(0, |acc, stream| acc + stream.length()) as usize;

        let start = stripe.offset() + start;
        reader
            .inner
            .seek(SeekFrom::Start(start))
            .context(error::IoSnafu)?;

        let mut scratch = vec![0; length];

        reader
            .inner
            .read_exact(&mut scratch)
            .context(error::IoSnafu)?;
        Ok(Self {
            data: Bytes::from(scratch),
            number_of_rows: stripe.number_of_rows(),
            compression,
            footer: footer.clone(),
            column: column.clone(),
            name: name.to_string(),
        })
    }

    pub fn stream(&self, kind: Kind) -> Option<Result<Decompressor>> {
        let mut start = 0; // the start of the stream

        let column_id = self.column.column_id() as u32;
        self.footer
            .streams
            .iter()
            .filter(|stream| stream.column() == column_id && stream.kind() != Kind::RowIndex)
            .map(|stream| {
                start += stream.length() as usize;
                stream
            })
            .find(|stream| stream.kind() == kind)
            .map(|stream| {
                let length = stream.length() as usize;
                let data = self.data.slice((start - length)..start);
                Decompressor::new(data, self.compression, vec![])
            })
            .map(Ok)
    }

    pub fn dictionary_size(&self) -> usize {
        let column = self.column.column_id();
        self.footer.columns[column]
            .dictionary_size
            .unwrap_or_default() as usize
    }

    pub fn encoding(&self) -> ColumnEncoding {
        let column = self.column.column_id();
        self.footer.columns[column]
    }

    pub fn number_of_rows(&self) -> usize {
        self.number_of_rows as usize
    }

    pub fn kind(&self) -> crate::proto::r#type::Kind {
        self.column.kind()
    }
}

pub struct NullableIterator<T> {
    present: Box<dyn Iterator<Item = bool>>,
    iter: Box<dyn Iterator<Item = Result<T>>>,
}

pub type GenericIterator<T> = NullableIterator<T>;

impl<T> Iterator for NullableIterator<T> {
    type Item = Result<Option<T>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.present.next() {
            Some(present) => {
                if present {
                    match self.iter.next() {
                        Some(Ok(value)) => Some(Ok(Some(value))),
                        Some(Err(err)) => Some(Err(err)),
                        None => None,
                    }
                } else {
                    Some(Ok(None))
                }
            }
            None => None,
        }
    }
}
