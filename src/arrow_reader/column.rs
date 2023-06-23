use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

use bytes::Bytes;
use snafu::{OptionExt, ResultExt};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};

use crate::error::{self, Result};
use crate::proto::stream::Kind;
use crate::proto::{ColumnEncoding, CompressionKind, StripeFooter, StripeInformation};
use crate::reader::decompress::Decompressor;
use crate::reader::schema::TypeDescription;
use crate::reader::Reader;

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

macro_rules! impl_read_stream {
    ($reader:ident,$start:ident,$length:ident $($_await:tt)*) => {{
        $reader
            .inner
            .seek(SeekFrom::Start($start))$($_await)*
            .context(error::IoSnafu)?;

        let mut scratch = vec![0; $length];

        $reader
            .inner
            .read_exact(&mut scratch)$($_await)*
            .context(error::IoSnafu)?;

        Ok(Bytes::from(scratch))
    }};
}

impl Column {
    pub fn read_stream<R: Read + Seek>(
        reader: &mut Reader<R>,
        start: u64,
        length: usize,
    ) -> Result<Bytes> {
        impl_read_stream!(reader, start, length)
    }

    pub async fn read_stream_async<R: AsyncRead + AsyncSeek + Unpin + Send>(
        reader: &mut Reader<R>,
        start: u64,
        length: usize,
    ) -> Result<Bytes> {
        impl_read_stream!(reader, start, length.await)
    }

    pub fn get_stream_info(
        name: &str,
        column: &Arc<TypeDescription>,
        footer: &Arc<StripeFooter>,
        stripe: &StripeInformation,
    ) -> Result<(u64, usize)> {
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

        Ok((start, length))
    }

    pub fn new<R: Read + Seek>(
        reader: &mut Reader<R>,
        compression: CompressionKind,
        name: &str,
        column: &Arc<TypeDescription>,
        footer: &Arc<StripeFooter>,
        stripe: &StripeInformation,
    ) -> Result<Self> {
        let (start, length) = Column::get_stream_info(name, column, footer, stripe)?;
        let data = Column::read_stream(reader, start, length)?;

        Ok(Self {
            data,
            number_of_rows: stripe.number_of_rows(),
            compression,
            footer: footer.clone(),
            column: column.clone(),
            name: name.to_string(),
        })
    }

    pub async fn new_async<R: AsyncRead + AsyncSeek + Unpin + Send>(
        reader: &mut Reader<R>,
        compression: CompressionKind,
        name: &str,
        column: &Arc<TypeDescription>,
        footer: &Arc<StripeFooter>,
        stripe: &StripeInformation,
    ) -> Result<Self> {
        let (start, length) = Column::get_stream_info(name, column, footer, stripe)?;
        let data = Column::read_stream_async(reader, start, length).await?;

        Ok(Self {
            data,
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
    present: Box<dyn Iterator<Item = bool> + Send>,
    iter: Box<dyn Iterator<Item = Result<T>> + Send>,
}

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
