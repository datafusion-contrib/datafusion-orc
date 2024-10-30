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

use std::sync::Arc;

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{FutureExt, TryFutureExt};
use orc_rust::reader::AsyncChunkReader;

use object_store::{ObjectMeta, ObjectStore};

/// Implements [`AsyncChunkReader`] to allow reading ORC files via `object_store` API.
pub struct ObjectStoreReader {
    store: Arc<dyn ObjectStore>,
    file: ObjectMeta,
}

impl ObjectStoreReader {
    pub fn new(store: Arc<dyn ObjectStore>, file: ObjectMeta) -> Self {
        Self { store, file }
    }
}

impl AsyncChunkReader for ObjectStoreReader {
    fn len(&mut self) -> BoxFuture<'_, std::io::Result<u64>> {
        async move { Ok(self.file.size as u64) }.boxed()
    }

    fn get_bytes(
        &mut self,
        offset_from_start: u64,
        length: u64,
    ) -> BoxFuture<'_, std::io::Result<Bytes>> {
        let offset_from_start = offset_from_start as usize;
        let length = length as usize;
        let range = offset_from_start..(offset_from_start + length);
        self.store
            .get_range(&self.file.location, range)
            .map_err(|e| e.into())
            .boxed()
    }
}
