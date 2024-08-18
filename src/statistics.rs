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

use crate::{error, proto};

/// Contains statistics for a specific column, for the entire file
/// or for a specific stripe.
#[derive(Debug, Clone)]
pub struct ColumnStatistics {
    number_of_values: u64,
    /// Use aid in 'IS NULL' predicates
    has_null: bool,
    type_statistics: Option<TypeStatistics>,
}

impl ColumnStatistics {
    pub fn number_of_values(&self) -> u64 {
        self.number_of_values
    }

    pub fn has_null(&self) -> bool {
        self.has_null
    }

    pub fn type_statistics(&self) -> Option<&TypeStatistics> {
        self.type_statistics.as_ref()
    }
}

#[derive(Debug, Clone)]
pub enum TypeStatistics {
    /// For TinyInt, SmallInt, Int and BigInt
    Integer {
        min: i64,
        max: i64,
        /// If sum overflows then recorded as None
        sum: Option<i64>,
    },
    /// For Float and Double
    Double {
        min: f64,
        max: f64,
        /// If sum overflows then recorded as None
        sum: Option<f64>,
    },
    String {
        min: String,
        max: String,
        /// Total length of all strings
        sum: i64,
    },
    /// For Boolean
    Bucket { true_count: u64 },
    Decimal {
        // TODO: use our own decimal type?
        min: String,
        max: String,
        sum: String,
    },
    Date {
        /// Days since epoch
        min: i32,
        max: i32,
    },
    Binary {
        // Total number of bytes across all values
        sum: i64,
    },
    Timestamp {
        /// Milliseconds since epoch
        /// These were used before ORC-135
        /// Where local timezone offset was included
        min: i64,
        max: i64,
        /// Milliseconds since UNIX epoch
        min_utc: i64,
        max_utc: i64,
    },
    Collection {
        min_children: u64,
        max_children: u64,
        total_children: u64,
    },
}

impl TryFrom<&proto::ColumnStatistics> for ColumnStatistics {
    type Error = error::OrcError;

    fn try_from(value: &proto::ColumnStatistics) -> Result<Self, Self::Error> {
        let type_statistics = if let Some(stats) = &value.int_statistics {
            Some(TypeStatistics::Integer {
                min: stats.minimum(),
                max: stats.maximum(),
                sum: stats.sum,
            })
        } else if let Some(stats) = &value.double_statistics {
            Some(TypeStatistics::Double {
                min: stats.minimum(),
                max: stats.maximum(),
                sum: stats.sum,
            })
        } else if let Some(stats) = &value.string_statistics {
            Some(TypeStatistics::String {
                min: stats.minimum().to_owned(),
                max: stats.maximum().to_owned(),
                sum: stats.sum(),
            })
        } else if let Some(stats) = &value.bucket_statistics {
            // TODO: false count?
            Some(TypeStatistics::Bucket {
                true_count: stats.count[0], // TODO: safety check this
            })
        } else if let Some(stats) = &value.decimal_statistics {
            Some(TypeStatistics::Decimal {
                min: stats.minimum().to_owned(),
                max: stats.maximum().to_owned(),
                sum: stats.sum().to_owned(),
            })
        } else if let Some(stats) = &value.date_statistics {
            Some(TypeStatistics::Date {
                min: stats.minimum(),
                max: stats.maximum(),
            })
        } else if let Some(stats) = &value.binary_statistics {
            Some(TypeStatistics::Binary { sum: stats.sum() })
        } else if let Some(stats) = &value.timestamp_statistics {
            Some(TypeStatistics::Timestamp {
                min: stats.minimum(),
                max: stats.maximum(),
                min_utc: stats.minimum_utc(),
                max_utc: stats.maximum_utc(),
            })
        } else {
            value
                .collection_statistics
                .as_ref()
                .map(|stats| TypeStatistics::Collection {
                    min_children: stats.min_children(),
                    max_children: stats.max_children(),
                    total_children: stats.total_children(),
                })
        };
        Ok(Self {
            number_of_values: value.number_of_values(),
            has_null: value.has_null(),
            type_statistics,
        })
    }
}
