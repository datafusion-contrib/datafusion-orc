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

use crate::physical_exec::OrcOpener;
use datafusion::common::DataFusionError;
use datafusion::datasource::physical_plan::{FileOpener, FileScanConfig, FileSource};
use datafusion::datasource::table_schema::TableSchema;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::projection::ProjectionExprs;
use object_store::ObjectStore;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct OrcSource {
    metrics: ExecutionPlanMetricsSet,
    batch_size: usize,
    table_schema: TableSchema,
}

impl OrcSource {
    pub fn new(table_schema: TableSchema) -> Self {
        Self {
            metrics: ExecutionPlanMetricsSet::default(),
            batch_size: 1024,
            table_schema,
        }
    }
}

impl FileSource for OrcSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        config: &FileScanConfig,
        _partition: usize,
    ) -> Result<Arc<dyn FileOpener>, DataFusionError> {
        OrcOpener::try_new(object_store, config, self.batch_size)
            .map(|f| Arc::new(f) as Arc<dyn FileOpener>)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(Self {
            batch_size,
            ..self.clone()
        })
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn file_type(&self) -> &str {
        "orc"
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>, DataFusionError> {
        todo!()
    }
}
