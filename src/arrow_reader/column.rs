use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

use arrow::datatypes::Field;
use bytes::Bytes;
use snafu::ResultExt;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};

use crate::error::{self, Result};
use crate::proto::{ColumnEncoding, StripeFooter};
use crate::reader::Reader;
use crate::schema::DataType;

pub mod binary;
pub mod boolean;
pub mod float;
pub mod int;
pub mod list;
pub mod map;
pub mod present;
pub mod string;
pub mod struct_column;
pub mod timestamp;
pub mod tinyint;

#[derive(Debug)]
pub struct Column {
    number_of_rows: u64,
    footer: Arc<StripeFooter>,
    name: String,
    data_type: DataType,
}

impl From<Column> for Field {
    fn from(value: Column) -> Self {
        let dt = value.data_type.to_arrow_data_type();
        Field::new(value.name, dt, true)
    }
}

impl From<&Column> for Field {
    fn from(value: &Column) -> Self {
        let dt = value.data_type.to_arrow_data_type();
        Field::new(value.name.clone(), dt, true)
    }
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
    pub fn new(
        name: &str,
        data_type: &DataType,
        footer: &Arc<StripeFooter>,
        number_of_rows: u64,
    ) -> Self {
        Self {
            number_of_rows,
            footer: footer.clone(),
            data_type: data_type.clone(),
            name: name.to_string(),
        }
    }

    pub fn dictionary_size(&self) -> usize {
        let column = self.data_type.column_index();
        self.footer.columns[column]
            .dictionary_size
            .unwrap_or_default() as usize
    }

    pub fn encoding(&self) -> ColumnEncoding {
        let column = self.data_type.column_index();
        self.footer.columns[column].clone()
    }

    pub fn number_of_rows(&self) -> usize {
        self.number_of_rows as usize
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn column_id(&self) -> u32 {
        self.data_type.column_index() as u32
    }

    pub fn children(&self) -> Vec<Column> {
        match &self.data_type {
            DataType::Boolean { .. }
            | DataType::Byte { .. }
            | DataType::Short { .. }
            | DataType::Int { .. }
            | DataType::Long { .. }
            | DataType::Float { .. }
            | DataType::Double { .. }
            | DataType::String { .. }
            | DataType::Varchar { .. }
            | DataType::Char { .. }
            | DataType::Binary { .. }
            | DataType::Decimal { .. }
            | DataType::Timestamp { .. }
            | DataType::TimestampWithLocalTimezone { .. }
            | DataType::Date { .. } => vec![],
            DataType::Struct { children, .. } => children
                .iter()
                .map(|(name, data_type)| Column {
                    number_of_rows: self.number_of_rows,
                    footer: self.footer.clone(),
                    name: name.clone(),
                    data_type: data_type.clone(),
                })
                .collect(),
            DataType::List { child, .. } => {
                vec![Column {
                    number_of_rows: self.number_of_rows,
                    footer: self.footer.clone(),
                    name: "item".to_string(),
                    data_type: *child.clone(),
                }]
            }
            DataType::Map { key, value, .. } => {
                vec![
                    Column {
                        number_of_rows: self.number_of_rows,
                        footer: self.footer.clone(),
                        name: "key".to_string(),
                        data_type: *key.clone(),
                    },
                    Column {
                        number_of_rows: self.number_of_rows,
                        footer: self.footer.clone(),
                        name: "value".to_string(),
                        data_type: *value.clone(),
                    },
                ]
            }
            DataType::Union { variants, .. } => {
                // TODO: might need corrections
                variants
                    .iter()
                    .enumerate()
                    .map(|(index, data_type)| Column {
                        number_of_rows: self.number_of_rows,
                        footer: self.footer.clone(),
                        name: format!("{index}"),
                        data_type: data_type.clone(),
                    })
                    .collect()
            }
        }
    }

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

impl<T> NullableIterator<T> {
    pub fn collect_chunk(&mut self, chunk: usize) -> Option<Result<Vec<Option<T>>>> {
        let mut buf = Vec::with_capacity(chunk);
        for _ in 0..chunk {
            match self.next() {
                Some(Ok(value)) => {
                    buf.push(value);
                }
                Some(Err(err)) => return Some(Err(err)),
                None => break,
            }
        }

        if buf.is_empty() {
            return None;
        }

        Some(Ok(buf))
    }
}
