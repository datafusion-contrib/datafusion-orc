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
