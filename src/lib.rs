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

//! Integration with [Apache DataFusion](https://datafusion.apache.org/) query engine to
//! allow querying ORC files with a SQL/DataFrame API.
//!
//! # Example usage
//!
//! ```no_run
//! # use datafusion::prelude::*;
//! # use datafusion::error::Result;
//! # use orc_rust::datafusion::{OrcReadOptions, SessionContextOrcExt};
//! # #[tokio::main]
//! # async fn main() -> Result<()> {
//! let ctx = SessionContext::new();
//! ctx.register_orc(
//!     "table1",
//!     "/path/to/file.orc",
//!     OrcReadOptions::default(),
//! )
//! .await?;
//!
//! ctx.sql("select a, b from table1")
//!     .await?
//!     .show()
//!     .await?;
//! # Ok(())
//! # }
//! ```

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::exec_err;
use datafusion::config::TableOptions;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::error::Result;
use datafusion::execution::config::SessionConfig;
use datafusion::execution::context::{DataFilePaths, SessionContext, SessionState};
use datafusion::execution::options::ReadOptions;

use async_trait::async_trait;

use self::file_format::OrcFormat;

mod file_format;
mod object_store_reader;
mod physical_exec;

/// Configuration options for reading ORC files.
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

/// Exposes new functions for registering ORC tables onto a DataFusion [`SessionContext`]
/// to enable querying them using the SQL or DataFrame API.
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

#[cfg(test)]
mod tests {
    use datafusion::assert_batches_sorted_eq;

    use super::*;

    #[tokio::test]
    async fn dataframe() -> Result<()> {
        let ctx = SessionContext::new();
        ctx.register_orc(
            "table1",
            "tests/basic/data/alltypes.snappy.orc",
            OrcReadOptions::default(),
        )
        .await?;

        let actual = ctx
            .sql("select int16, utf8 from table1 limit 5")
            .await?
            .collect()
            .await?;

        assert_batches_sorted_eq!(
            [
                "+-------+--------+",
                "| int16 | utf8   |",
                "+-------+--------+",
                "|       |        |",
                "| -1    |        |",
                "| 0     |        |",
                "| 1     | a      |",
                "| 32767 | encode |",
                "+-------+--------+",
            ],
            &actual
        );

        Ok(())
    }
}
