use std::{error::Error, fs::File, path::PathBuf, sync::Arc};

use clap::Parser;
use datafusion_orc::{reader::metadata::read_metadata, stripe::Stripe};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// ORC file path
    file: PathBuf,

    /// Display data for all stripes
    #[arg(short, long)]
    stripes: bool,
}

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();

    let mut f = File::open(cli.file)?;
    let metadata = Arc::new(read_metadata(&mut f)?);

    // TODO: better way to handle this printing?
    println!(
        "compression: {}",
        metadata
            .compression()
            .map(|c| c.to_string())
            .unwrap_or("None".to_string())
    );
    println!("file format version: {}", metadata.file_format_version());
    println!("number of rows: {}", metadata.number_of_rows());
    println!("number of stripes: {}", metadata.stripe_metadatas().len());

    // TODO: nesting types indentation is messed up
    println!("schema:\n{}", metadata.root_data_type());
    if cli.stripes {
        println!("\n=== Stripes ===");
        for (i, stripe_metadata) in metadata.stripe_metadatas().iter().enumerate() {
            let stripe = Stripe::new(
                &mut f,
                &metadata,
                metadata.root_data_type(),
                i,
                stripe_metadata,
            )?;
            println!("stripe index: {i}");
            println!("number of rows: {}", stripe.number_of_rows());
            println!(
                "writer timezone: {}",
                stripe
                    .writer_tz()
                    .map(|tz| tz.to_string())
                    .unwrap_or("None".to_string())
            );
            println!();
        }
    }

    Ok(())
}
