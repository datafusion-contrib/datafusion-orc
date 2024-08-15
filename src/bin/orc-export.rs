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

use std::{fs::File, io, path::PathBuf};

use anyhow::Result;
use arrow::{array::RecordBatch, csv, datatypes::DataType, error::ArrowError, json};
use clap::{Parser, ValueEnum};
use json::writer::{JsonFormat, LineDelimited};
use orc_rust::{projection::ProjectionMask, reader::metadata::read_metadata, ArrowReaderBuilder};

#[derive(Parser)]
#[command(name = "orc-export")]
#[command(version, about = "Export data from orc file to csv", long_about = None)]
struct Cli {
    /// Path to the orc file
    file: PathBuf,
    /// Output file. If not provided output will be printed on console
    #[arg(short, long)]
    output_file: Option<PathBuf>,
    /// Output format. If not provided then the output is csv
    #[arg(value_enum, short, long, default_value_t = FileFormat::Csv)]
    format: FileFormat,
    /// export only first N records
    #[arg(short, long, value_name = "N")]
    num_rows: Option<u64>,
    /// export only provided columns. Comma separated list
    #[arg(short, long, value_delimiter = ',')]
    columns: Option<Vec<String>>,
}

#[derive(Clone, Debug, PartialEq, ValueEnum)]
enum FileFormat {
    /// Output data in csv format
    Csv,
    /// Output data in json format
    Json,
}

#[allow(clippy::large_enum_variant)]
enum OutputWriter<W: io::Write, F: JsonFormat> {
    Csv(csv::Writer<W>),
    Json(json::Writer<W, F>),
}

impl<W, F> OutputWriter<W, F>
where
    W: io::Write,
    F: JsonFormat,
{
    fn write(&mut self, batch: &RecordBatch) -> Result<(), ArrowError> {
        match self {
            OutputWriter::Csv(w) => w.write(batch),
            OutputWriter::Json(w) => w.write(batch),
        }
    }

    fn finish(&mut self) -> Result<(), ArrowError> {
        match self {
            OutputWriter::Csv(_) => Ok(()),
            OutputWriter::Json(w) => w.finish(),
        }
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Prepare reader
    let mut f = File::open(&cli.file)?;
    let metadata = read_metadata(&mut f)?;

    // Select columns which should be exported (Binary and Decimal are not supported)
    let cols: Vec<usize> = metadata
        .root_data_type()
        .children()
        .iter()
        .enumerate()
        // TODO: handle nested types
        .filter(|(_, nc)| match nc.data_type().to_arrow_data_type() {
            DataType::Binary => false,
            DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => {
                matches!(cli.format, FileFormat::Csv)
            }
            _ => {
                if let Some(cols) = &cli.columns {
                    cols.iter().any(|c| nc.name().eq(c))
                } else {
                    true
                }
            }
        })
        .map(|(i, _)| i)
        .collect();

    let projection = ProjectionMask::roots(metadata.root_data_type(), cols);
    let reader = ArrowReaderBuilder::try_new(f)?
        .with_projection(projection)
        .build();

    // Prepare writer
    let writer: Box<dyn io::Write> = if let Some(output) = cli.output_file {
        Box::new(File::create(output)?)
    } else {
        Box::new(io::stdout())
    };

    let mut output_writer = match cli.format {
        FileFormat::Json => {
            OutputWriter::Json(json::WriterBuilder::new().build::<_, LineDelimited>(writer))
        }
        _ => OutputWriter::Csv(csv::WriterBuilder::new().with_header(true).build(writer)),
    };

    // Convert data
    let mut num_rows = cli.num_rows.unwrap_or(u64::MAX);
    for mut batch in reader.flatten() {
        // Restrict rows
        if num_rows < batch.num_rows() as u64 {
            batch = batch.slice(0, num_rows as usize);
        }

        // Save
        output_writer.write(&batch)?;

        // Have we reached limit on the number of rows?
        if num_rows > batch.num_rows() as u64 {
            num_rows -= batch.num_rows() as u64;
        } else {
            break;
        }
    }

    output_writer.finish()?;

    Ok(())
}
