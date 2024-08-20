use std::io::Write;

use arrow::{
    array::RecordBatch,
    datatypes::{DataType as ArrowDataType, SchemaRef},
};
use prost::Message;
use snafu::{ensure, ResultExt};

use crate::{
    error::{IoSnafu, Result, UnexpectedSnafu},
    proto,
    writer::{
        column::EstimateMemory,
        stripe::{StripeInformation, StripeWriter},
    },
};

/// Construct an [`ArrowWriter`] to encode [`RecordBatch`]es into a single
/// ORC file.
pub struct ArrowWriterBuilder<W> {
    writer: W,
    schema: SchemaRef,
    batch_size: usize,
    stripe_byte_size: usize,
}

impl<W: Write> ArrowWriterBuilder<W> {
    /// Create a new [`ArrowWriterBuilder`], which will write an ORC file to
    /// the provided writer, with the expected Arrow schema.
    pub fn new(writer: W, schema: SchemaRef) -> Self {
        Self {
            writer,
            schema,
            batch_size: 1024,
            // 64 MiB
            stripe_byte_size: 64 * 1024 * 1024,
        }
    }

    /// Batch size controls the encoding behaviour, where `batch_size` values
    /// are encoded at a time. Default is `1024`.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// The approximate size of stripes. Default is `64MiB`.
    pub fn with_stripe_byte_size(mut self, stripe_byte_size: usize) -> Self {
        self.stripe_byte_size = stripe_byte_size;
        self
    }

    /// Construct an [`ArrowWriter`] ready to encode [`RecordBatch`]es into
    /// an ORC file.
    pub fn try_build(mut self) -> Result<ArrowWriter<W>> {
        // Required magic "ORC" bytes at start of file
        self.writer.write_all(b"ORC").context(IoSnafu)?;
        let writer = StripeWriter::new(self.writer, &self.schema);
        Ok(ArrowWriter {
            writer,
            schema: self.schema,
            batch_size: self.batch_size,
            stripe_byte_size: self.stripe_byte_size,
            written_stripes: vec![],
            // Accounting for the 3 magic bytes above
            total_bytes_written: 3,
        })
    }
}

/// Encodes [`RecordBatch`]es into an ORC file. Will encode `batch_size` rows
/// at a time into a stripe, flushing the stripe to the underlying writer when
/// it's estimated memory footprint exceeds the configures `stripe_byte_size`.
pub struct ArrowWriter<W> {
    writer: StripeWriter<W>,
    schema: SchemaRef,
    batch_size: usize,
    stripe_byte_size: usize,
    written_stripes: Vec<StripeInformation>,
    /// Used to keep track of progress in file so far (instead of needing Seek on the writer)
    total_bytes_written: u64,
}

impl<W: Write> ArrowWriter<W> {
    /// Encode the provided batch at `batch_size` rows at a time, flushing any
    /// stripes that exceed the configured stripe size.
    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        ensure!(
            batch.schema() == self.schema,
            UnexpectedSnafu {
                msg: "RecordBatch doesn't match expected schema"
            }
        );

        for offset in (0..batch.num_rows()).step_by(self.batch_size) {
            let length = self.batch_size.min(batch.num_rows() - offset);
            let batch = batch.slice(offset, length);
            self.writer.encode_batch(&batch)?;

            // Flush stripe when it exceeds estimated configured size
            if self.writer.estimate_memory_size() > self.stripe_byte_size {
                self.flush_stripe()?;
            }
        }
        Ok(())
    }

    /// Flush any buffered data that hasn't been written, and write the stripe
    /// footer metadata.
    pub fn flush_stripe(&mut self) -> Result<()> {
        let info = self.writer.finish_stripe(self.total_bytes_written)?;
        self.total_bytes_written += info.total_byte_size();
        self.written_stripes.push(info);
        Ok(())
    }

    /// Flush the current stripe if it is still in progress, and write the tail
    /// metadata and close the writer.
    pub fn close(mut self) -> Result<()> {
        // Flush in-progress stripe
        if self.writer.row_count > 0 {
            self.flush_stripe()?;
        }
        let footer = serialize_footer(&self.written_stripes, &self.schema);
        let footer = footer.encode_to_vec();
        let postscript = serialize_postscript(footer.len() as u64);
        let postscript = postscript.encode_to_vec();
        let postscript_len = postscript.len() as u8;

        let mut writer = self.writer.finish();
        writer.write_all(&footer).context(IoSnafu)?;
        writer.write_all(&postscript).context(IoSnafu)?;
        // Postscript length as last byte
        writer.write_all(&[postscript_len]).context(IoSnafu)?;

        // TODO: return file metadata
        Ok(())
    }
}

fn serialize_schema(schema: &SchemaRef) -> Vec<proto::Type> {
    let mut types = vec![];

    let field_names = schema
        .fields()
        .iter()
        .map(|f| f.name().to_owned())
        .collect();
    // TODO: consider nested types
    let subtypes = (1..(schema.fields().len() as u32 + 1)).collect();
    let root_type = proto::Type {
        kind: Some(proto::r#type::Kind::Struct.into()),
        subtypes,
        field_names,
        maximum_length: None,
        precision: None,
        scale: None,
        attributes: vec![],
    };
    types.push(root_type);
    for field in schema.fields() {
        let t = match field.data_type() {
            ArrowDataType::Float32 => proto::Type {
                kind: Some(proto::r#type::Kind::Float.into()),
                ..Default::default()
            },
            ArrowDataType::Float64 => proto::Type {
                kind: Some(proto::r#type::Kind::Double.into()),
                ..Default::default()
            },
            ArrowDataType::Int8 => proto::Type {
                kind: Some(proto::r#type::Kind::Byte.into()),
                ..Default::default()
            },
            ArrowDataType::Int16 => proto::Type {
                kind: Some(proto::r#type::Kind::Short.into()),
                ..Default::default()
            },
            ArrowDataType::Int32 => proto::Type {
                kind: Some(proto::r#type::Kind::Int.into()),
                ..Default::default()
            },
            ArrowDataType::Int64 => proto::Type {
                kind: Some(proto::r#type::Kind::Long.into()),
                ..Default::default()
            },
            // TODO: support more types
            _ => unimplemented!("unsupported datatype"),
        };
        types.push(t);
    }
    types
}

fn serialize_footer(stripes: &[StripeInformation], schema: &SchemaRef) -> proto::Footer {
    let body_length = stripes
        .iter()
        .map(|s| s.index_length + s.data_length + s.footer_length)
        .sum::<u64>();
    let number_of_rows = stripes.iter().map(|s| s.row_count as u64).sum::<u64>();
    let stripes = stripes.iter().map(From::from).collect();
    let types = serialize_schema(schema);
    proto::Footer {
        header_length: Some(3),
        content_length: Some(body_length + 3),
        stripes,
        types,
        metadata: vec![],
        number_of_rows: Some(number_of_rows),
        statistics: vec![],
        row_index_stride: None,
        writer: Some(u32::MAX),
        encryption: None,
        calendar: None,
        software_version: None,
    }
}

fn serialize_postscript(footer_length: u64) -> proto::PostScript {
    proto::PostScript {
        footer_length: Some(footer_length),
        compression: Some(proto::CompressionKind::None.into()), // TODO: support compression
        compression_block_size: None,
        version: vec![0, 12],
        metadata_length: Some(0),       // TODO: statistics
        writer_version: Some(u32::MAX), // TODO: check which version to use
        stripe_statistics_length: None,
        magic: Some("ORC".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{
            Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
            RecordBatchReader,
        },
        compute::concat_batches,
        datatypes::{DataType as ArrowDataType, Field, Schema},
    };
    use bytes::Bytes;

    use crate::ArrowReaderBuilder;

    use super::*;

    #[test]
    fn test_roundtrip_write() {
        let f32_array = Arc::new(Float32Array::from(vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0]));
        let f64_array = Arc::new(Float64Array::from(vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0]));
        let int8_array = Arc::new(Int8Array::from(vec![0, 1, 2, 3, 4, 5, 6]));
        let int16_array = Arc::new(Int16Array::from(vec![0, 1, 2, 3, 4, 5, 6]));
        let int32_array = Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4, 5, 6]));
        let int64_array = Arc::new(Int64Array::from(vec![0, 1, 2, 3, 4, 5, 6]));
        let schema = Schema::new(vec![
            Field::new("f32", ArrowDataType::Float32, false),
            Field::new("f64", ArrowDataType::Float64, false),
            Field::new("int8", ArrowDataType::Int8, false),
            Field::new("int16", ArrowDataType::Int16, false),
            Field::new("int32", ArrowDataType::Int32, false),
            Field::new("int64", ArrowDataType::Int64, false),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                f32_array,
                f64_array,
                int8_array,
                int16_array,
                int32_array,
                int64_array,
            ],
        )
        .unwrap();

        let mut f = vec![];
        let mut writer = ArrowWriterBuilder::new(&mut f, batch.schema())
            .try_build()
            .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let f = Bytes::from(f);
        let reader = ArrowReaderBuilder::try_new(f).unwrap().build();
        let rows = reader.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(batch, rows[0]);
    }

    #[test]
    fn test_write_small_stripes() {
        // Set small stripe size to ensure writing across multiple stripes works
        let data: Vec<i64> = (0..1_000_000).collect();
        let int64_array = Arc::new(Int64Array::from(data));
        let schema = Schema::new(vec![Field::new("int64", ArrowDataType::Int64, true)]);

        let batch = RecordBatch::try_new(Arc::new(schema), vec![int64_array]).unwrap();

        let mut f = vec![];
        let mut writer = ArrowWriterBuilder::new(&mut f, batch.schema())
            .with_stripe_byte_size(256)
            .try_build()
            .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let f = Bytes::from(f);
        let reader = ArrowReaderBuilder::try_new(f).unwrap().build();
        let schema = reader.schema();
        // Current reader doesn't read a batch across stripe boundaries, so we expect
        // more than one batch to prove multiple stripes are being written here
        let rows = reader.collect::<Result<Vec<_>, _>>().unwrap();
        assert!(
            rows.len() > 1,
            "must have written more than 1 stripe (each stripe read as separate recordbatch)"
        );
        let actual = concat_batches(&schema, rows.iter()).unwrap();
        assert_eq!(batch, actual);
    }
}
