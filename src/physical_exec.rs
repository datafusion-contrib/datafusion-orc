// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;

use arrow::error::ArrowError;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::physical_plan::{FileOpenFuture, FileOpener, FileScanConfig};
use datafusion::error::Result;
use datafusion_datasource::PartitionedFile;
use orc_rust::projection::ProjectionMask;
use orc_rust::ArrowReaderBuilder;

use futures_util::{StreamExt, TryStreamExt};
use object_store::ObjectStore;

use super::object_store_reader::ObjectStoreReader;

pub(crate) struct OrcOpener {
    projection: Vec<usize>,
    batch_size: usize,
    table_schema: SchemaRef,
    object_store: Arc<dyn ObjectStore>,
}

impl OrcOpener {
    pub(crate) fn new(
        object_store: Arc<dyn ObjectStore>,
        config: &FileScanConfig,
        batch_size: usize,
    ) -> Self {
        let projection = config
            .file_column_projection_indices()
            .unwrap_or_else(|| (0..config.file_schema().fields().len()).collect());

        Self {
            projection,
            batch_size: config.batch_size.unwrap_or(batch_size),
            table_schema: config.file_schema().clone(),
            object_store,
        }
    }
}

impl FileOpener for OrcOpener {
    fn open(&self, file: PartitionedFile) -> Result<FileOpenFuture> {
        let object_meta = &file.object_meta;
        let reader = ObjectStoreReader::new(self.object_store.clone(), object_meta.clone());
        let batch_size = self.batch_size;
        let projected_schema = SchemaRef::from(self.table_schema.project(&self.projection)?);

        Ok(Box::pin(async move {
            let mut builder = ArrowReaderBuilder::try_new_async(reader)
                .await
                .map_err(ArrowError::from)?;
            // Find complex data type column index as projection
            let mut projection = Vec::with_capacity(projected_schema.fields().len());
            for named_column in builder.file_metadata().root_data_type().children() {
                if let Some((_table_idx, _table_field)) =
                    projected_schema.fields().find(named_column.name())
                {
                    projection.push(named_column.data_type().column_index());
                }
            }
            let projection_mask =
                ProjectionMask::roots(builder.file_metadata().root_data_type(), projection);
            if let Some(range) = file.range.clone() {
                let range = range.start as usize..range.end as usize;
                builder = builder.with_file_byte_range(range);
            }
            let reader = builder
                .with_batch_size(batch_size)
                .with_projection(projection_mask)
                .build_async();

            Ok(reader.map_err(Into::into).boxed())
        }))
    }
}
