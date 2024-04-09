use std::any::Any;
use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::error::ArrowError;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{
    FileMeta, FileOpenFuture, FileOpener, FileScanConfig, FileStream,
};
use datafusion::datasource::{provider_as_source, TableProvider, TableType};
use datafusion::error::Result;
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion::prelude::*;
use datafusion_expr::{Expr, LogicalPlanBuilder};
use datafusion_physical_expr::EquivalenceProperties;
use orc_rust::ArrowReaderBuilder;

use async_trait::async_trait;
use futures_util::StreamExt;
use object_store::{GetResultPayload, ObjectStore};

#[tokio::main]
async fn main() -> Result<()> {
    let path = Path::new("tests/basic/data/alltypes.snappy.orc").canonicalize()?;
    let orc = OrcDataSource::new(path.to_str().unwrap())?;

    let ctx = SessionContext::new();

    let logical_plan = LogicalPlanBuilder::scan_with_filters(
        "orc_table",
        provider_as_source(Arc::new(orc)),
        None,
        vec![],
    )?
    .build()?;

    DataFrame::new(ctx.state(), logical_plan)
        // TODO: support projection pushdown
        // .select_columns(&["int16", "utf8"])?
        .show()
        .await?;

    Ok(())
}

#[derive(Clone, Debug)]
struct OrcDataSource {
    path: String,
    schema: SchemaRef,
}

impl OrcDataSource {
    fn new(path: impl AsRef<str>) -> Result<Self> {
        let path = path.as_ref().to_owned();
        let f = File::open(path.clone())?;
        let builder = ArrowReaderBuilder::try_new(f).map_err(ArrowError::from)?;
        let metadata = builder.file_metadata().to_owned();
        let schema = metadata
            .root_data_type()
            .create_arrow_schema(&HashMap::default());
        let schema = Arc::new(schema);
        Ok(Self { path, schema })
    }
}

#[async_trait]
impl TableProvider for OrcDataSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(OrcExec::new(self.clone(), self.schema())?))
    }
}

#[derive(Debug, Clone)]
struct OrcExec {
    orc: OrcDataSource,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl OrcExec {
    fn new(orc: OrcDataSource, schema: SchemaRef) -> Result<Self> {
        Ok(Self {
            orc,
            schema: schema.clone(),
            properties: PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(1),
                ExecutionMode::Bounded,
            ),
        })
    }
}

impl DisplayAs for OrcExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "OrcExec")
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

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let partitioned_file = PartitionedFile::from_path(self.orc.path.clone())?;
        let config = FileScanConfig {
            object_store_url: ObjectStoreUrl::local_filesystem(),
            file_schema: self.schema.clone(),
            file_groups: vec![vec![partitioned_file]],
            statistics: self.statistics()?,
            projection: None,
            limit: None,
            table_partition_cols: vec![],
            output_ordering: vec![],
        };
        let object_store = context
            .runtime_env()
            .object_store(&config.object_store_url)?;

        let opener = OrcOpener { object_store };

        let stream = FileStream::new(&config, partition, opener, &Default::default())?;
        Ok(Box::pin(stream))
    }
}

struct OrcOpener {
    object_store: Arc<dyn ObjectStore>,
}

impl FileOpener for OrcOpener {
    fn open(&self, file_meta: FileMeta) -> Result<FileOpenFuture> {
        let object_store = self.object_store.clone();
        Ok(Box::pin(async move {
            let r = object_store.get(file_meta.location()).await?;
            match r.payload {
                GetResultPayload::File(file, _) => {
                    let reader = ArrowReaderBuilder::try_new(file)
                        .map_err(ArrowError::from)?
                        .build();
                    Ok(futures::stream::iter(reader).boxed())
                }
                GetResultPayload::Stream(_) => todo!(),
            }
        }))
    }
}
