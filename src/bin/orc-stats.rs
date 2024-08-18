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

use std::{fs::File, path::PathBuf, sync::Arc};

use anyhow::Result;
use arrow::temporal_conversions::{date32_to_datetime, timestamp_ms_to_datetime};
use clap::Parser;
use orc_rust::{reader::metadata::read_metadata, statistics::ColumnStatistics};

#[derive(Parser)]
#[command(name = "orc-stats")]
#[command(version, about = "Print column and stripe stats from the orc file", long_about = None)]
struct Cli {
    /// Path to the orc file
    file: PathBuf,
}

fn print_column_stats(col_stats: &ColumnStatistics) {
    if let Some(tstats) = col_stats.type_statistics() {
        match tstats {
            orc_rust::statistics::TypeStatistics::Integer { min, max, sum } => {
                println!("* Data type Integer");
                println!("* Minimum: {}", min);
                println!("* Maximum: {}", max);
                if let Some(sum) = sum {
                    println!("* Sum: {}", sum);
                }
            }
            orc_rust::statistics::TypeStatistics::Double { min, max, sum } => {
                println!("* Data type Double");
                println!("* Minimum: {}", min);
                println!("* Maximum: {}", max);
                if let Some(sum) = sum {
                    println!("* Sum: {}", sum);
                }
            }
            orc_rust::statistics::TypeStatistics::String { min, max, sum } => {
                println!("* Data type String");
                println!("* Minimum: {}", min);
                println!("* Maximum: {}", max);
                println!("* Sum: {}", sum);
            }
            orc_rust::statistics::TypeStatistics::Bucket { true_count } => {
                println!("* Data type Bucket");
                println!("* True count: {}", true_count);
            }
            orc_rust::statistics::TypeStatistics::Decimal { min, max, sum } => {
                println!("* Data type Decimal");
                println!("* Minimum: {}", min);
                println!("* Maximum: {}", max);
                println!("* Sum: {}", sum);
            }
            orc_rust::statistics::TypeStatistics::Date { min, max } => {
                println!("* Data type Date");
                if let Some(dt) = date32_to_datetime(*min) {
                    println!("* Minimum: {}", dt);
                }
                if let Some(dt) = date32_to_datetime(*max) {
                    println!("* Maximum: {}", dt);
                }
            }
            orc_rust::statistics::TypeStatistics::Binary { sum } => {
                println!("* Data type Binary");
                println!("* Sum: {}", sum);
            }
            orc_rust::statistics::TypeStatistics::Timestamp {
                min,
                max,
                min_utc,
                max_utc,
            } => {
                println!("* Data type Timestamp");
                println!("* Minimum: {}", min);
                println!("* Maximum: {}", max);
                if let Some(ts) = timestamp_ms_to_datetime(*min_utc) {
                    println!("* Minimum UTC: {}", ts);
                }
                if let Some(ts) = timestamp_ms_to_datetime(*max_utc) {
                    println!("* Maximum UTC: {}", ts);
                }
            }
            orc_rust::statistics::TypeStatistics::Collection {
                min_children,
                max_children,
                total_children,
            } => {
                println!("* Data type Collection");
                println!("* Minimum children: {}", min_children);
                println!("* Maximum children: {}", max_children);
                println!("* Total children: {}", total_children);
            }
        }
    }

    println!("* Num values: {}", col_stats.number_of_values());
    println!("* Has nulls: {}", col_stats.has_null());
    println!();
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    let mut f = File::open(&cli.file)?;
    let metadata = Arc::new(read_metadata(&mut f)?);

    println!("# Column stats");
    println!(
        "File {:?} has {} columns",
        cli.file,
        metadata.column_file_statistics().len()
    );
    println!();
    for (idx, col_stats) in metadata.column_file_statistics().iter().enumerate() {
        println!("## Column {idx}");
        print_column_stats(col_stats);
    }

    println!("# Stripe stats");
    println!(
        "File {:?} has {} stripes",
        cli.file,
        metadata.stripe_metadatas().len()
    );
    println!();
    for (idm, sm) in metadata.stripe_metadatas().iter().enumerate() {
        println!("----- Stripe {idm} -----\n");
        for (idc, col_stats) in sm.column_statistics().iter().enumerate() {
            println!("## Column {idc}");
            print_column_stats(col_stats);
        }
    }

    Ok(())
}
