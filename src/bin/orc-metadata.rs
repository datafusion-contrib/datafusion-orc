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

use std::{error::Error, fs::File, path::PathBuf, sync::Arc};

use clap::Parser;
use orc_rust::{reader::metadata::read_metadata, stripe::Stripe};

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
