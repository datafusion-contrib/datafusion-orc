use std::sync::Arc;

use crate::reader::AsyncChunkReader;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{FutureExt, TryFutureExt};

use object_store::{ObjectMeta, ObjectStore};

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
