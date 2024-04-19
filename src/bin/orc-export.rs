use std::{fs::File, io, path::PathBuf};

use anyhow::Result;
use arrow::{array::RecordBatch, csv, error::ArrowError, json};
use clap::{Parser, ValueEnum};
use json::writer::{JsonFormat, LineDelimited};
use orc_rust::ArrowReaderBuilder;

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
    #[arg(value_enum, short, long)]
    format: Option<FileFormat>,
    /// export only first N records
    #[arg(short, long, value_name = "N")]
    num_rows: Option<i64>,
    // TODO: convert_dates
    // TODO: columns="col1,col2"
}

#[derive(Clone, Debug, ValueEnum)]
enum FileFormat {
    /// Output data in csv format
    Csv,
    /// Output data in json format
    Json,
}

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
    let f = File::open(&cli.file)?;
    let reader = ArrowReaderBuilder::try_new(f)?.build();

    // Prepare writer
    let writer: Box<dyn io::Write> = if let Some(output) = cli.output_file {
        Box::new(File::create(output)?)
    } else {
        Box::new(io::stdout())
    };

    let mut output_writer = match cli.format {
        Some(FileFormat::Json) => {
            OutputWriter::Json(json::WriterBuilder::new().build::<_, LineDelimited>(writer))
        }
        _ => OutputWriter::Csv(csv::WriterBuilder::new().with_header(true).build(writer)),
    };

    // Convert data
    let mut num_rows = cli.num_rows.unwrap_or(i64::MAX);
    for mut batch in reader.flatten() {
        if num_rows < batch.num_rows() as i64 {
            batch = batch.slice(0, num_rows as usize);
        }

        output_writer.write(&batch)?;

        num_rows = num_rows - batch.num_rows() as i64;
        if num_rows <= 0 {
            break;
        }
    }

    output_writer.finish()?;

    Ok(())
}
