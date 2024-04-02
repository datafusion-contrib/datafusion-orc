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

use std::fs::File;

use criterion::{criterion_group, criterion_main, Criterion};
use futures_util::TryStreamExt;
use orc_rust::arrow_reader::ArrowReaderBuilder;

fn basic_path(path: &str) -> String {
    let dir = env!("CARGO_MANIFEST_DIR");
    format!("{}/tests/basic/data/{}", dir, path)
}

// demo-12-zlib.orc
// 1,920,800 total rows
// Columns:
//   - Int32
//   - Dictionary(UInt64, Utf8)
//   - Dictionary(UInt64, Utf8)
//   - Dictionary(UInt64, Utf8)
//   - Int32
//   - Dictionary(UInt64, Utf8)
//   - Int32
//   - Int32
//   - Int32

async fn async_read_all() {
    let file = "demo-12-zlib.orc";
    let file_path = basic_path(file);
    let f = tokio::fs::File::open(file_path).await.unwrap();
    let reader = ArrowReaderBuilder::try_new_async(f)
        .await
        .unwrap()
        .build_async();
    let _ = reader.try_collect::<Vec<_>>().await.unwrap();
}

fn sync_read_all() {
    let file = "demo-12-zlib.orc";
    let file_path = basic_path(file);
    let f = File::open(file_path).unwrap();
    let reader = ArrowReaderBuilder::try_new(f).unwrap().build();
    let _ = reader.collect::<Result<Vec<_>, _>>().unwrap();
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("sync reader", |b| b.iter(sync_read_all));
    c.bench_function("async reader", |b| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter(async_read_all);
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
