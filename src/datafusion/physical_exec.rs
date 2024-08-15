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

use std::any::Any;
use std::fmt::{self, Debug};
use std::sync::Arc;

use crate::projection::ProjectionMask;
use crate::ArrowReaderBuilder;
use arrow::error::ArrowError;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::physical_plan::{
    FileMeta, FileOpenFuture, FileOpener, FileScanConfig, FileStream,
};
use datafusion::error::Result;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_physical_expr::EquivalenceProperties;

use futures_util::StreamExt;
use object_store::ObjectStore;

use super::object_store_reader::ObjectStoreReader;

#[derive(Debug, Clone)]
pub struct OrcExec {
    config: FileScanConfig,
    metrics: ExecutionPlanMetricsSet,
    properties: PlanProperties,
}

impl OrcExec {
    pub fn new(config: FileScanConfig) -> Self {
        let metrics = ExecutionPlanMetricsSet::new();
        let (projected_schema, _, orderings) = config.project();
        let properties = PlanProperties::new(
            EquivalenceProperties::new_with_orderings(projected_schema, &orderings),
            Partitioning::UnknownPartitioning(config.file_groups.len()),
            ExecutionMode::Bounded,
        );
        Self {
            config,
            metrics,
            properties,
        }
    }
}

impl DisplayAs for OrcExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "OrcExec: ")?;
        self.config.fmt_as(t, f)
    }
}

impl ExecutionPlan for OrcExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn execute(
        &self,
        partition_index: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let projection: Vec<_> = self
            .config
            .projection
            .as_ref()
            .map(|p| {
                // FileScanConfig::file_column_projection_indices
                p.iter()
                    .filter(|col_idx| **col_idx < self.config.file_schema.fields().len())
                    .copied()
                    .collect()
            })
            .unwrap_or_else(|| (0..self.config.file_schema.fields().len()).collect());

        let object_store = context
            .runtime_env()
            .object_store(&self.config.object_store_url)?;

        let opener = OrcOpener {
            _partition_index: partition_index,
            projection,
            batch_size: context.session_config().batch_size(),
            _limit: self.config.limit,
            _table_schema: self.config.file_schema.clone(),
            _metrics: self.metrics.clone(),
            object_store,
        };

        let stream = FileStream::new(&self.config, partition_index, opener, &self.metrics)?;
        Ok(Box::pin(stream))
    }
}

// TODO: make use of the unused fields (e.g. implement metrics)
struct OrcOpener {
    _partition_index: usize,
    projection: Vec<usize>,
    batch_size: usize,
    _limit: Option<usize>,
    _table_schema: SchemaRef,
    _metrics: ExecutionPlanMetricsSet,
    object_store: Arc<dyn ObjectStore>,
}

impl FileOpener for OrcOpener {
    fn open(&self, file_meta: FileMeta) -> Result<FileOpenFuture> {
        let reader =
            ObjectStoreReader::new(self.object_store.clone(), file_meta.object_meta.clone());
        let batch_size = self.batch_size;
        // Offset by 1 since index 0 is the root
        let projection = self.projection.iter().map(|i| i + 1).collect::<Vec<_>>();
        Ok(Box::pin(async move {
            let builder = ArrowReaderBuilder::try_new_async(reader)
                .await
                .map_err(ArrowError::from)?;
            let projection_mask =
                ProjectionMask::roots(builder.file_metadata().root_data_type(), projection);
            let reader = builder
                .with_batch_size(batch_size)
                .with_projection(projection_mask)
                .build_async();

            Ok(reader.boxed())
        }))
    }
}
