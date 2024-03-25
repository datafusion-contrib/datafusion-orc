use std::{error::Error, fs::File, path::PathBuf};

use clap::Parser;
use datafusion_orc::ArrowReaderBuilder;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// ORC file path
    file: PathBuf,
}

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();

    let f = File::open(cli.file)?;
    let builder = ArrowReaderBuilder::try_new(f)?;
    let metadata = builder.file_metadata();

    // TODO: better way to handle this printing?
    // TODO: move this display to actual library
    println!(
        "compression: {}",
        metadata
            .compression()
            .map(|c| c.compression_type().to_string())
            .unwrap_or("None".to_string())
    );
    println!("number of rows: {}", metadata.number_of_rows());
    println!("number of stripes: {}", metadata.stripe_metadatas().len());
    // TODO: nesting types indentation is messed up
    println!("schema:\n{}", metadata.root_data_type());

    Ok(())
}
