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

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileOpenFuture, FileOpener, FileScanConfig};
use datafusion::error::Result;
use orc_rust::projection::ProjectionMask;
use orc_rust::ArrowReaderBuilder;

use futures_util::{StreamExt, TryStreamExt};
use object_store::ObjectStore;

use super::object_store_reader::ObjectStoreReader;

pub(crate) struct OrcOpener {
    projected_schema: SchemaRef,
    batch_size: usize,
    object_store: Arc<dyn ObjectStore>,
}

impl OrcOpener {
    pub(crate) fn try_new(
        object_store: Arc<dyn ObjectStore>,
        config: &FileScanConfig,
        batch_size: usize,
    ) -> Result<Self> {
        Ok(Self {
            projected_schema: config.projected_schema()?,
            batch_size: config.batch_size.unwrap_or(batch_size),
            object_store,
        })
    }
}

impl FileOpener for OrcOpener {
    fn open(&self, file: PartitionedFile) -> Result<FileOpenFuture> {
        let object_meta = &file.object_meta;
        let reader = ObjectStoreReader::new(self.object_store.clone(), object_meta.clone());
        let batch_size = self.batch_size;
        let projected_schema = self.projected_schema.clone();

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
