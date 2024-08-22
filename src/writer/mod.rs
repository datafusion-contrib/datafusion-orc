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

use std::fmt::Debug;

use bytes::Bytes;

use crate::proto;

pub mod column;
pub mod stripe;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum StreamType {
    Present,
    Data,
    Length,
    DictionaryData,
    Secondary,
}

impl From<StreamType> for proto::stream::Kind {
    fn from(value: StreamType) -> Self {
        match value {
            StreamType::Present => proto::stream::Kind::Present,
            StreamType::Data => proto::stream::Kind::Data,
            StreamType::Length => proto::stream::Kind::Length,
            StreamType::DictionaryData => proto::stream::Kind::DictionaryData,
            StreamType::Secondary => proto::stream::Kind::Secondary,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Stream {
    kind: StreamType,
    bytes: Bytes,
}

impl Stream {
    pub fn into_parts(self) -> (StreamType, Bytes) {
        (self.kind, self.bytes)
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ColumnEncoding {
    Direct,
    DirectV2,
    Dictionary { size: usize },
    DictionaryV2 { size: usize },
}

impl From<&ColumnEncoding> for proto::ColumnEncoding {
    fn from(value: &ColumnEncoding) -> Self {
        match value {
            ColumnEncoding::Direct => proto::ColumnEncoding {
                kind: Some(proto::column_encoding::Kind::Direct.into()),
                dictionary_size: None,
                bloom_encoding: None,
            },
            ColumnEncoding::DirectV2 => proto::ColumnEncoding {
                kind: Some(proto::column_encoding::Kind::DirectV2.into()),
                dictionary_size: None,
                bloom_encoding: None,
            },
            ColumnEncoding::Dictionary { size } => proto::ColumnEncoding {
                kind: Some(proto::column_encoding::Kind::Dictionary.into()),
                dictionary_size: Some(*size as u32),
                bloom_encoding: None,
            },
            ColumnEncoding::DictionaryV2 { size } => proto::ColumnEncoding {
                kind: Some(proto::column_encoding::Kind::DictionaryV2.into()),
                dictionary_size: Some(*size as u32),
                bloom_encoding: None,
            },
        }
    }
}
