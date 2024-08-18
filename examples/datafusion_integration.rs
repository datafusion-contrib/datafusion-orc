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

use datafusion::error::Result;
use datafusion::prelude::*;
use orc_rust::datafusion::{OrcReadOptions, SessionContextOrcExt};

#[tokio::main]
async fn main() -> Result<()> {
    // Make sure to import SessionContextOrcExt which makes these register/read ORC
    // methods available on SessionContext. With that done, we are able to process
    // ORC files using SQL or the DataFrame API.
    let ctx = SessionContext::new();
    ctx.register_orc(
        "table1",
        "tests/basic/data/alltypes.snappy.orc",
        OrcReadOptions::default(),
    )
    .await?;

    ctx.sql("select int16, utf8 from table1")
        .await?
        .show()
        .await?;

    ctx.sql("select count(*) from table1").await?.show().await?;

    ctx.read_orc(
        "tests/basic/data/alltypes.snappy.orc",
        OrcReadOptions::default(),
    )
    .await?
    .select_columns(&["int16", "utf8"])?
    .show()
    .await?;

    Ok(())
}
