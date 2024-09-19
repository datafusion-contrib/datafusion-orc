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

import shutil
import glob
from datetime import date as dt
from decimal import Decimal as Dec
from pyspark.sql import SparkSession
from pyspark.sql.types import *

dir = "tests/basic/data"

# We're using Spark because it supports lzo compression writing
# (PyArrow supports all except lzo writing)

spark = SparkSession.builder.getOrCreate()

# TODO: how to do char and varchar?
# TODO: struct, list, map, union
df = spark.createDataFrame(
    [ #   bool, int8,         int16,         int32,         int64,       float32,        float64,               decimal,              binary,       utf8,           date32
        ( None, None,          None,          None,          None,          None,           None,                  None,                None,       None,             None),
        ( True,    0,             0,             0,             0,           0.0,            0.0,                Dec(0),         "".encode(),         "", dt(1970,  1,  1)),
        (False,    1,             1,             1,             1,           1.0,            1.0,                Dec(1),        "a".encode(),        "a", dt(1970,  1,  2)),
        (False,   -1,            -1,            -1,            -1,          -1.0,           -1.0,               Dec(-1),        " ".encode(),        " ", dt(1969, 12, 31)),
        ( True,  127, (1 << 15) - 1, (1 << 31) - 1, (1 << 63) - 1,  float("inf"),   float("inf"),  Dec(123456789.12345),   "encode".encode(),   "encode", dt(9999, 12, 31)),
        ( True, -128,    -(1 << 15),    -(1 << 31),    -(1 << 63), float("-inf"),  float("-inf"), Dec(-999999999.99999),   "decode".encode(),   "decode", dt(1582, 10, 15)),
        ( True,   50,            50,            50,            50,     3.1415927,  3.14159265359,       Dec(-31256.123), "大熊和奏".encode(), "大熊和奏", dt(1582, 10, 16)),
        ( True,   51,            51,            51,            51,    -3.1415927, -3.14159265359,          Dec(1241000), "斉藤朱夏".encode(), "斉藤朱夏", dt(2000,  1,  1)),
        ( True,   52,            52,            52,            52,           1.1,            1.1,              Dec(1.1), "鈴原希実".encode(), "鈴原希実", dt(3000, 12, 31)),
        (False,   53,            53,            53,            53,          -1.1,           -1.1,          Dec(0.99999),       "🤔".encode(),       "🤔", dt(1900,  1,  1)),
        ( None, None,          None,          None,          None,          None,           None,                  None,                None,       None,             None),
    ],
    StructType(
        [
            StructField("boolean",      BooleanType()),
            StructField(   "int8",         ByteType()),
            StructField(  "int16",        ShortType()),
            StructField(  "int32",      IntegerType()),
            StructField(  "int64",         LongType()),
            StructField("float32",        FloatType()),
            StructField("float64",       DoubleType()),
            StructField("decimal", DecimalType(15, 5)),
            StructField( "binary",       BinaryType()),
            StructField(   "utf8",       StringType()),
            StructField( "date32",         DateType()),
        ]
    ),
).coalesce(1)

compression = ["none", "snappy", "zlib", "lzo", "zstd", "lz4"]
for c in compression:
    df.write.format("orc")\
      .option("compression", c)\
      .mode("overwrite")\
      .save(f"{dir}/alltypes.{c}")
    # Since Spark saves into a directory
    # Move out and rename the expected single ORC file (because of coalesce above)
    orc_file = glob.glob(f"{dir}/alltypes.{c}/*.orc")[0]
    shutil.move(orc_file, f"{dir}/alltypes.{c}.orc")
    shutil.rmtree(f"{dir}/alltypes.{c}")
