use std::marker::PhantomData;

use arrow::{
    array::{Array, ArrayRef, AsArray},
    datatypes::{ArrowPrimitiveType, Int8Type, ToByteSlice},
};
use bytes::BytesMut;

use crate::{
    error::Result,
    reader::decode::{byte_rle::ByteRleWriter, float::Float},
    writer::StreamType,
};

use super::{ColumnEncoding, PresentStreamEncoder, Stream};

/// Encodes a specific column for a stripe. Will encode to an internal memory
/// buffer until it is finished, in which case it returns the stream bytes to
/// be serialized to a writer.
pub trait ColumnStripeEncoder {
    /// Encode entire provided [`ArrayRef`] to internal buffer.
    fn encode_array(&mut self, array: &ArrayRef) -> Result<()>;

    /// Column encoding used for streams.
    fn column_encoding(&self) -> ColumnEncoding;

    /// Approximate current memory usage to aid in determining when to flush
    /// a stripe to the writer.
    fn estimate_memory_size(&self) -> usize;

    /// Emit buffered streams to be written to the writer, and reset state
    /// in preparation for next stripe.
    fn finish(&mut self) -> Vec<Stream>;
}

/// ORC float/double column encoder.
pub struct FloatStripeEncoder<T: ArrowPrimitiveType>
where
    T::Native: Float,
{
    data: BytesMut,
    /// Lazily initialized once we encounter an [`Array`] with a [`NullBuffer`].
    present: Option<PresentStreamEncoder>,
    phantom: PhantomData<T>,
}

impl<T: ArrowPrimitiveType> FloatStripeEncoder<T>
where
    T::Native: Float,
{
    pub fn new() -> Self {
        Self {
            data: BytesMut::new(),
            present: None,
            phantom: Default::default(),
        }
    }
}

impl<T: ArrowPrimitiveType> ColumnStripeEncoder for FloatStripeEncoder<T>
where
    T::Native: Float,
{
    fn encode_array(&mut self, array: &ArrayRef) -> Result<()> {
        let array = array.as_primitive::<T>();
        // Handling case where if encoding across RecordBatch boundaries, arrays
        // might introduce a NullBuffer
        match (array.nulls(), &mut self.present) {
            // Need to copy only the valid values as indicated by null_buffer
            (Some(null_buffer), Some(present)) => {
                present.extend(null_buffer);
                for index in null_buffer.valid_indices() {
                    let f = array.value(index);
                    let f = f.to_byte_slice();
                    self.data.extend_from_slice(f);
                }
            }
            (Some(null_buffer), None) => {
                let mut present = PresentStreamEncoder::new();
                present.extend(null_buffer);
                self.present = Some(present);
                for index in null_buffer.valid_indices() {
                    let f = array.value(index);
                    let f = f.to_byte_slice();
                    self.data.extend_from_slice(f);
                }
            }
            // Simple direct copy from values buffer, extending present if needed
            (None, Some(present)) => {
                let bytes = array.values().to_byte_slice();
                self.data.extend_from_slice(bytes);
                present.extend_present(array.len());
            }
            (None, None) => {
                let bytes = array.values().to_byte_slice();
                self.data.extend_from_slice(bytes);
            }
        }

        Ok(())
    }

    fn column_encoding(&self) -> ColumnEncoding {
        ColumnEncoding::Direct
    }

    fn estimate_memory_size(&self) -> usize {
        self.data.len()
            + self
                .present
                .as_ref()
                .map(|p| p.estimate_memory_size())
                .unwrap_or(0)
    }

    fn finish(&mut self) -> Vec<Stream> {
        // TODO: Figure out how to retain the allocation instead of handing
        //       it off each time.
        let bytes = std::mem::take(&mut self.data);
        // Return mandatory Data stream and optional Present stream
        let data = Stream {
            s_type: StreamType::Data,
            bytes: bytes.into(),
        };
        match &mut self.present {
            Some(present) => {
                let bytes = present.finish();
                let present = Stream {
                    s_type: StreamType::Present,
                    bytes,
                };
                vec![data, present]
            }
            None => vec![data],
        }
    }
}

/// ORC TinyInt column encoder.
pub struct ByteStripeEncoder {
    encoder: ByteRleWriter,
    /// Lazily initialized once we encounter an [`Array`] with a [`NullBuffer`].
    present: Option<PresentStreamEncoder>,
}

impl ByteStripeEncoder {
    pub fn new() -> Self {
        Self {
            encoder: ByteRleWriter::new(),
            present: None,
        }
    }
}

impl ColumnStripeEncoder for ByteStripeEncoder {
    fn encode_array(&mut self, array: &ArrayRef) -> Result<()> {
        let array = array.as_primitive::<Int8Type>();
        // Handling case where if encoding across RecordBatch boundaries, arrays
        // might introduce a NullBuffer
        match (array.nulls(), &mut self.present) {
            // Need to copy only the valid values as indicated by null_buffer
            (Some(null_buffer), Some(present)) => {
                present.extend(null_buffer);
                for index in null_buffer.valid_indices() {
                    let b = array.value(index) as u8;
                    self.encoder.write_byte(b);
                }
            }
            (Some(null_buffer), None) => {
                let mut present = PresentStreamEncoder::new();
                present.extend(null_buffer);
                self.present = Some(present);
                for index in null_buffer.valid_indices() {
                    let b = array.value(index) as u8;
                    self.encoder.write_byte(b);
                }
            }
            // Simple direct copy from values buffer, extending present if needed
            (None, Some(present)) => {
                let bytes = array.values().to_byte_slice();
                self.encoder.write(bytes);
                present.extend_present(array.len());
            }
            (None, None) => {
                let bytes = array.values().to_byte_slice();
                self.encoder.write(bytes);
            }
        }

        Ok(())
    }

    fn column_encoding(&self) -> ColumnEncoding {
        ColumnEncoding::Direct
    }

    fn estimate_memory_size(&self) -> usize {
        self.encoder.estimate_memory_size()
            + self
                .present
                .as_ref()
                .map(|p| p.estimate_memory_size())
                .unwrap_or(0)
    }

    fn finish(&mut self) -> Vec<Stream> {
        // TODO: Figure out how to retain the allocation instead of handing
        //       it off each time.
        let bytes = self.encoder.take_inner();
        // Return mandatory Data stream and optional Present stream
        let data = Stream {
            s_type: StreamType::Data,
            bytes: bytes.into(),
        };
        match &mut self.present {
            Some(present) => {
                let bytes = present.finish();
                let present = Stream {
                    s_type: StreamType::Present,
                    bytes,
                };
                vec![data, present]
            }
            None => vec![data],
        }
    }
}
