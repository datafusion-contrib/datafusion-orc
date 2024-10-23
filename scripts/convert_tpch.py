# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import pyarrow as pa
from pyarrow import orc
from pyarrow import csv

tables = [
    "customer",
    "lineitem",
    "nation",
    "orders",
    "part",
    "partsupp",
    "region",
    "supplier"
]

# Datatypes based on:
# https://github.com/apache/datafusion/blob/3b93cc952b889cec2364ad2490ae18ecddb3ca49/benchmarks/src/tpch/mod.rs#L50-L134
schemas = {
    "customer": pa.schema([
        pa.field("c_custkey",    pa.int64()),
        pa.field("c_name",       pa.string()),
        pa.field("c_address",    pa.string()),
        pa.field("c_nationkey",  pa.int64()),
        pa.field("c_phone",      pa.string()),
        pa.field("c_acctbal",    pa.decimal128(15, 2)),
        pa.field("c_mktsegment", pa.string()),
        pa.field("c_comment",    pa.string()),
    ]),
    "lineitem": pa.schema([
        pa.field("l_orderkey",      pa.int64()),
        pa.field("l_partkey",       pa.int64()),
        pa.field("l_suppkey",       pa.int64()),
        pa.field("l_linenumber",    pa.int32()),
        pa.field("l_quantity",      pa.decimal128(15, 2)),
        pa.field("l_extendedprice", pa.decimal128(15, 2)),
        pa.field("l_discount",      pa.decimal128(15, 2)),
        pa.field("l_tax",           pa.decimal128(15, 2)),
        pa.field("l_returnflag",    pa.string()),
        pa.field("l_linestatus",    pa.string()),
        pa.field("l_shipdate",      pa.date32()),
        pa.field("l_commitdate",    pa.date32()),
        pa.field("l_receiptdate",   pa.date32()),
        pa.field("l_shipinstruct",  pa.string()),
        pa.field("l_shipmode",      pa.string()),
        pa.field("l_comment",       pa.string()),
    ]),
    "nation": pa.schema([
        pa.field("n_nationkey", pa.int64()),
        pa.field("n_name",      pa.string()),
        pa.field("n_regionkey", pa.int64()),
        pa.field("n_comment",   pa.string()),
    ]),
    "orders": pa.schema([
        pa.field("o_orderkey",      pa.int64()),
        pa.field("o_custkey",       pa.int64()),
        pa.field("o_orderstatus",   pa.string()),
        pa.field("o_totalprice",    pa.decimal128(15, 2)),
        pa.field("o_orderdate",     pa.date32()),
        pa.field("o_orderpriority", pa.string()),
        pa.field("o_clerk",         pa.string()),
        pa.field("o_shippriority",  pa.int32()),
        pa.field("o_comment",       pa.string()),
    ]),
    "part": pa.schema([
        pa.field("p_partkey",     pa.int64()),
        pa.field("p_name",        pa.string()),
        pa.field("p_mfgr",        pa.string()),
        pa.field("p_brand",       pa.string()),
        pa.field("p_type",        pa.string()),
        pa.field("p_size",        pa.int32()),
        pa.field("p_container",   pa.string()),
        pa.field("p_retailprice", pa.decimal128(15, 2)),
        pa.field("p_comment",     pa.string()),
    ]),
    "partsupp": pa.schema([
        pa.field("ps_partkey",    pa.int64()),
        pa.field("ps_suppkey",    pa.int64()),
        pa.field("ps_availqty",   pa.int32()),
        pa.field("ps_supplycost", pa.decimal128(15, 2)),
        pa.field("ps_comment",    pa.string()),
    ]),
    "region": pa.schema([
        pa.field("r_regionkey", pa.int64()),
        pa.field("r_name",      pa.string()),
        pa.field("r_comment",   pa.string()),
    ]),
    "supplier": pa.schema([
        pa.field("s_suppkey",   pa.int64()),
        pa.field("s_name",      pa.string()),
        pa.field("s_address",   pa.string()),
        pa.field("s_nationkey", pa.int64()),
        pa.field("s_phone",     pa.string()),
        pa.field("s_acctbal",   pa.decimal128(15, 2)),
        pa.field("s_comment",   pa.string()),
    ]),
}

for table in tables:
    schema = schemas[table]
    tbl = csv.read_csv(
        f"benchmark_data/{table}.tbl",
        read_options=csv.ReadOptions(column_names=schema.names),
        parse_options=csv.ParseOptions(delimiter="|"),
        convert_options=csv.ConvertOptions(column_types=schema),
    )
    orc.write_table(tbl, f"benchmark_data/{table}.orc")
