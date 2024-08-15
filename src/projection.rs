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

use crate::schema::RootDataType;

// TODO: be able to nest project (project columns within struct type)

/// Specifies which column indices to project from an ORC type.
#[derive(Debug, Clone)]
pub struct ProjectionMask {
    /// Indices of column in ORC type, can refer to nested types
    /// (not only root level columns)
    indices: Option<Vec<usize>>,
}

impl ProjectionMask {
    /// Project all columns.
    pub fn all() -> Self {
        Self { indices: None }
    }

    /// Project only specific columns from the root type by column index.
    pub fn roots(root_data_type: &RootDataType, indices: impl IntoIterator<Item = usize>) -> Self {
        // TODO: return error if column index not found?
        let input_indices = indices.into_iter().collect::<Vec<_>>();
        // By default always project root
        let mut indices = vec![0];
        root_data_type
            .children()
            .iter()
            .filter(|col| input_indices.contains(&col.data_type().column_index()))
            .for_each(|col| indices.extend(col.data_type().all_indices()));
        Self {
            indices: Some(indices),
        }
    }

    /// Project only specific columns from the root type by column name.
    pub fn named_roots<T>(root_data_type: &RootDataType, names: &[T]) -> Self
    where
        T: AsRef<str>,
    {
        // TODO: return error if column name not found?
        // By default always project root
        let mut indices = vec![0];
        let names = names.iter().map(AsRef::as_ref).collect::<Vec<_>>();
        root_data_type
            .children()
            .iter()
            .filter(|col| names.contains(&col.name()))
            .for_each(|col| indices.extend(col.data_type().all_indices()));
        Self {
            indices: Some(indices),
        }
    }

    /// Check if ORC column should is projected or not, by index.
    pub fn is_index_projected(&self, index: usize) -> bool {
        match &self.indices {
            Some(indices) => indices.contains(&index),
            None => true,
        }
    }
}
