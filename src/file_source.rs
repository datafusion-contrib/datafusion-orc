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
use datafusion::common::Statistics;
use datafusion::datasource::physical_plan::{FileOpener, FileScanConfig, FileSource};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_datasource::TableSchema;
use object_store::ObjectStore;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct OrcSource {
    metrics: ExecutionPlanMetricsSet,
    statistics: Statistics,
    batch_size: usize,
}

impl Default for OrcSource {
    fn default() -> Self {
        Self {
            metrics: ExecutionPlanMetricsSet::default(),
            statistics: Statistics::default(),
            batch_size: 1024,
        }
    }
}

impl FileSource for OrcSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        config: &FileScanConfig,
        _partition: usize,
    ) -> Arc<dyn FileOpener> {
        Arc::new(OrcOpener::new(object_store, config, self.batch_size))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(Self {
            batch_size,
            ..self.clone()
        })
    }

    fn with_schema(&self, _schema: TableSchema) -> Arc<dyn FileSource> {
        Arc::new(self.clone())
    }

    fn with_projection(&self, _config: &FileScanConfig) -> Arc<dyn FileSource> {
        Arc::new(self.clone())
    }

    fn with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource> {
        Arc::new(Self {
            statistics,
            ..self.clone()
        })
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn statistics(&self) -> datafusion::common::Result<Statistics> {
        Ok(self.statistics.clone())
    }

    fn file_type(&self) -> &str {
        "orc"
    }
}
