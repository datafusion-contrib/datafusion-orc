use std::{fs::File, io, path::PathBuf};

use anyhow::Result;
use arrow::csv;
use clap::Parser;
use orc_rust::ArrowReaderBuilder;

#[derive(Parser)]
#[command(name = "orc-export")]
#[command(version, about = "Export data from orc file to csv", long_about = None)]
struct Cli {
    /// Path to the orc file
    file: PathBuf,
    /// Output file
    #[arg(short, long)]
    output: Option<PathBuf>,
    // TODO: head=N
    // TODO: convert_dates
    // TODO: format=[csv|json]
    // TODO: columns="col1,col2"
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let f = File::open(&cli.file)?;
    let output_writer: Box<dyn io::Write> = if let Some(output) = cli.output {
        Box::new(File::create(output).unwrap())
    } else {
        Box::new(io::stdout())
    };

    let reader = ArrowReaderBuilder::try_new(f).unwrap().build();
    let mut writer = csv::WriterBuilder::new()
        .with_header(true)
        .build(output_writer);

    for batch in reader.flatten() {
        writer.write(&batch)?;
    }

    Ok(())
}
