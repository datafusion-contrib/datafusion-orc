use std::any::Any;
use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::sync::Arc;

use crate::projection::ProjectionMask;
use crate::reader::metadata::read_metadata_async;
use crate::reader::AsyncChunkReader;
use crate::ArrowReaderBuilder;
use arrow::datatypes::Schema;
use arrow::error::ArrowError;
use bytes::Bytes;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{exec_err, FileType, Statistics};
use datafusion::config::TableOptions;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::physical_plan::{
    FileMeta, FileOpenFuture, FileOpener, FileScanConfig, FileStream,
};
use datafusion::error::Result;
use datafusion::execution::context::{DataFilePaths, SessionState, TaskContext};
use datafusion::execution::options::ReadOptions;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion::prelude::*;
use datafusion_physical_expr::{EquivalenceProperties, PhysicalExpr};
use futures::future::BoxFuture;
use futures::{FutureExt, TryFutureExt, TryStreamExt};

use async_trait::async_trait;
use futures_util::StreamExt;
use object_store::{ObjectMeta, ObjectStore};

#[derive(Clone)]
pub struct OrcReadOptions<'a> {
    pub file_extension: &'a str,
}

impl<'a> Default for OrcReadOptions<'a> {
    fn default() -> Self {
        Self {
            file_extension: "orc",
        }
    }
}

#[async_trait]
impl ReadOptions<'_> for OrcReadOptions<'_> {
    fn to_listing_options(
        &self,
        _config: &SessionConfig,
        _table_options: TableOptions,
    ) -> ListingOptions {
        let file_format = OrcFormat::new();
        ListingOptions::new(Arc::new(file_format)).with_file_extension(self.file_extension)
    }

    async fn get_resolved_schema(
        &self,
        config: &SessionConfig,
        state: SessionState,
        table_path: ListingTableUrl,
    ) -> Result<SchemaRef> {
        self._get_resolved_schema(config, state, table_path, None)
            .await
    }
}

pub trait SessionContextOrcExt {
    fn read_orc<P: DataFilePaths + Send>(
        &self,
        table_paths: P,
        options: OrcReadOptions<'_>,
    ) -> impl std::future::Future<Output = Result<DataFrame>> + Send;

    fn register_orc(
        &self,
        name: &str,
        table_path: &str,
        options: OrcReadOptions<'_>,
    ) -> impl std::future::Future<Output = Result<()>> + Send;
}

impl SessionContextOrcExt for SessionContext {
    async fn read_orc<P: DataFilePaths + Send>(
        &self,
        table_paths: P,
        options: OrcReadOptions<'_>,
    ) -> Result<DataFrame> {
        // SessionContext::_read_type
        let table_paths = table_paths.to_urls()?;
        let session_config = self.copied_config();
        let listing_options =
            ListingOptions::new(Arc::new(OrcFormat::new())).with_file_extension(".orc");

        let option_extension = listing_options.file_extension.clone();

        if table_paths.is_empty() {
            return exec_err!("No table paths were provided");
        }

        // check if the file extension matches the expected extension
        for path in &table_paths {
            let file_path = path.as_str();
            if !file_path.ends_with(option_extension.clone().as_str()) && !path.is_collection() {
                return exec_err!(
                    "File path '{file_path}' does not match the expected extension '{option_extension}'"
                );
            }
        }

        let resolved_schema = options
            .get_resolved_schema(&session_config, self.state(), table_paths[0].clone())
            .await?;
        let config = ListingTableConfig::new_with_multi_paths(table_paths)
            .with_listing_options(listing_options)
            .with_schema(resolved_schema);
        let provider = ListingTable::try_new(config)?;
        self.read_table(Arc::new(provider))
    }

    async fn register_orc(
        &self,
        name: &str,
        table_path: &str,
        options: OrcReadOptions<'_>,
    ) -> Result<()> {
        let listing_options =
            options.to_listing_options(&self.copied_config(), self.copied_table_options());
        self.register_listing_table(name, table_path, listing_options, None, None)
            .await?;
        Ok(())
    }
}

async fn fetch_schema(
    store: &Arc<dyn ObjectStore>,
    file: &ObjectMeta,
) -> Result<(object_store::path::Path, Schema)> {
    let loc_path = file.location.clone();
    let mut reader = ObjectStoreReader::new(Arc::clone(store), file.clone());
    let metadata = read_metadata_async(&mut reader)
        .await
        .map_err(ArrowError::from)?;
    let schema = metadata
        .root_data_type()
        .create_arrow_schema(&HashMap::default());
    Ok((loc_path, schema))
}

struct ObjectStoreReader {
    store: Arc<dyn ObjectStore>,
    file: ObjectMeta,
}

impl ObjectStoreReader {
    pub fn new(store: Arc<dyn ObjectStore>, file: ObjectMeta) -> Self {
        Self { store, file }
    }
}

impl AsyncChunkReader for ObjectStoreReader {
    fn len(&mut self) -> BoxFuture<'_, std::io::Result<u64>> {
        async move { Ok(self.file.size as u64) }.boxed()
    }

    fn get_bytes(
        &mut self,
        offset_from_start: u64,
        length: u64,
    ) -> BoxFuture<'_, std::io::Result<Bytes>> {
        let offset_from_start = offset_from_start as usize;
        let length = length as usize;
        let range = offset_from_start..(offset_from_start + length);
        self.store
            .get_range(&self.file.location, range)
            .map_err(|e| e.into())
            .boxed()
    }
}

#[derive(Clone, Debug)]
struct OrcFormat {}

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

#[derive(Debug, Clone)]
struct OrcExec {
    config: FileScanConfig,
    metrics: ExecutionPlanMetricsSet,
    properties: PlanProperties,
}

impl OrcExec {
    fn new(config: FileScanConfig) -> Self {
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

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
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
