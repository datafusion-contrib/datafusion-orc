use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use crate::reader::metadata::read_metadata_async;
use arrow::datatypes::Schema;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{FileType, Statistics};
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_physical_expr::PhysicalExpr;
use futures::TryStreamExt;

use async_trait::async_trait;
use futures_util::StreamExt;
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore};

use super::object_store_reader::ObjectStoreReader;
use super::physical_exec::OrcExec;

async fn fetch_schema(store: &Arc<dyn ObjectStore>, file: &ObjectMeta) -> Result<(Path, Schema)> {
    let loc_path = file.location.clone();
    let mut reader = ObjectStoreReader::new(Arc::clone(store), file.clone());
    let metadata = read_metadata_async(&mut reader).await?;
    let schema = metadata
        .root_data_type()
        .create_arrow_schema(&HashMap::default());
    Ok((loc_path, schema))
}

#[derive(Clone, Debug)]
pub struct OrcFormat {}

impl OrcFormat {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl FileFormat for OrcFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn infer_schema(
        &self,
        state: &SessionState,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        let mut schemas: Vec<_> = futures::stream::iter(objects)
            .map(|object| fetch_schema(store, object))
            .boxed() // Workaround https://github.com/rust-lang/rust/issues/64552
            .buffered(state.config_options().execution.meta_fetch_concurrency)
            .try_collect()
            .await?;

        // Schema inference adds fields based the order they are seen
        // which depends on the order the files are processed. For some
        // object stores (like local file systems) the order returned from list
        // is not deterministic. Thus, to ensure deterministic schema inference
        // sort the files first.
        // https://github.com/apache/datafusion/pull/6629
        schemas.sort_by(|(location1, _), (location2, _)| location1.cmp(location2));

        let schemas = schemas
            .into_iter()
            .map(|(_, schema)| schema)
            .collect::<Vec<_>>();

        let schema = Schema::try_merge(schemas)?;

        Ok(Arc::new(schema))
    }

    async fn infer_stats(
        &self,
        _state: &SessionState,
        _store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&table_schema))
    }

    async fn create_physical_plan(
        &self,
        _state: &SessionState,
        conf: FileScanConfig,
        _filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(OrcExec::new(conf)))
    }

    // TODO: Doesn't seem important for now, but maybe change in DataFusion itself?
    //       Add an Extension(name) variant maybe?
    fn file_type(&self) -> FileType {
        unimplemented!("Extension file type: ORC")
    }
}
