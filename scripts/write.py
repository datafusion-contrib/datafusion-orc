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

# Copied from https://github.com/DataEngineeringLabs/orc-format/blob/416490db0214fc51d53289253c0ee91f7fc9bc17/write.py
import random
import datetime
import pyorc

dir = "tests/basic/data"

data = {
    "a": [1.0, 2.0, None, 4.0, 5.0],
    "b": [True, False, None, True, False],
    "str_direct": ["a", "cccccc", None, "ddd", "ee"],
    "d": ["a", "bb", None, "ccc", "ddd"],
    "e": ["ddd", "cc", None, "bb", "a"],
    "f": ["aaaaa", "bbbbb", None, "ccccc", "ddddd"],
    "int_short_repeated": [5, 5, None, 5, 5],
    "int_neg_short_repeated": [-5, -5, None, -5, -5],
    "int_delta": [1, 2, None, 4, 5],
    "int_neg_delta": [5, 4, None, 2, 1],
    "int_direct": [1, 6, None, 3, 2],
    "int_neg_direct": [-1, -6, None, -3, -2],
    "bigint_direct": [1, 6, None, 3, 2],
    "bigint_neg_direct": [-1, -6, None, -3, -2],
    "bigint_other": [5, -5, 1, 5, 5],
    "utf8_increase": ["a", "bb", "ccc", "dddd", "eeeee"],
    "utf8_decrease": ["eeeee", "dddd", "ccc", "bb", "a"],
    "timestamp_simple": [datetime.datetime(2023, 4, 1, 20, 15, 30, 2000), datetime.datetime.fromtimestamp(int('1629617204525777000')/1000000000), datetime.datetime(2023, 1, 1), datetime.datetime(2023, 2, 1), datetime.datetime(2023, 3, 1)],
    "date_simple": [datetime.date(2023, 4, 1), datetime.date(2023, 3, 1), datetime.date(2023, 1, 1), datetime.date(2023, 2, 1), datetime.date(2023, 3, 1)],
    "tinyint_simple": [-1, None, 1, 127, -127]
}

def infer_schema(data):
    schema = "struct<"
    for key, value in data.items():
        dt = type(value[0])
        if dt == float:
            dt = "float"
        elif dt == int:
            dt = "int"
        elif dt == bool:
            dt = "boolean"
        elif dt == str:
            dt = "string"
        elif dt == dict: 
            dt = infer_schema(value[0])
        elif key.startswith("timestamp"):
            dt = "timestamp"
        elif key.startswith("date"):
            dt = "date"
        else:
            print(key,value,dt)
            raise NotImplementedError
        if key.startswith("double"):
            dt = "double"
        if key.startswith("bigint"):
            dt = "bigint"
        if key.startswith("tinyint"):
            dt = "tinyint"
        schema += key + ":" + dt + ","

    schema = schema[:-1] + ">"
    return schema



def _write(
    schema: str,
    data,
    file_name: str,
    compression=pyorc.CompressionKind.NONE,
    dict_key_size_threshold=0.0,
):
    output = open(file_name, "wb")
    writer = pyorc.Writer(
        output,
        schema,
        dict_key_size_threshold=dict_key_size_threshold,
        # use a small number to ensure that compression crosses value boundaries
        compression_block_size=32,
        compression=compression,
    )
    num_rows = len(list(data.values())[0])
    for x in range(num_rows):
        row = tuple(values[x] for values in data.values())
        writer.write(row)
    writer.close()

    with open(file_name, "rb") as f:
        reader = pyorc.Reader(f)
        list(reader)

nested_struct = {
    "nest": [
        (1.0,True),
        (3.0,None),
        (None,None),
        None,
        (-3.0,None)
    ],
}

_write("struct<nest:struct<a:float,b:boolean>>", nested_struct, f"{dir}/nested_struct.orc")


nested_array = {
    "value": [
        [1, None, 3, 43, 5],
        [5, None, 32, 4, 15],
        [16, None, 3, 4, 5, 6],
        None,
        [3, None],
    ],
}

_write("struct<value:array<int>>", nested_array, f"{dir}/nested_array.orc")


nested_array_float = {
    "value": [
        [1.0, 3.0],
        [None, 2.0],
    ],
}

_write("struct<value:array<float>>", nested_array_float, f"{dir}/nested_array_float.orc")

nested_array_struct = {
    "value": [
        [(1.0, 1, "01"), (2.0, 2, "02")],
        [None, (3.0, 3, "03")],
    ],
}

_write("struct<value:array<struct<a:float,b:int,c:string>>>", nested_array_struct, f"{dir}/nested_array_struct.orc")

nested_map = {
    "map": [
            {"zero": 0, "one": 1},
            None,
            {"two": 2, "tree": 3},
            {"one": 1, "two": 2, "nill": None},
    ],
}

_write("struct<map:map<string,int>>", nested_map, f"{dir}/nested_map.orc")

nested_map_struct = {
    "map": [
        {"01": (1.0, 1, "01"), "02": (2.0, 1, "02")},
        None,
        {"03": (3.0, 3, "03"), "04": (4.0, 4, "04")},
    ],
}

_write("struct<value:map<string,struct<a:float,b:int,c:string>>>", nested_map_struct, f"{dir}/nested_map_struct.orc")


_write(
    infer_schema(data),
    data,
    f"{dir}/test.orc",
)

data_boolean = {
    "long": [True] * 32,
}

_write("struct<long:boolean>", data_boolean, f"{dir}/long_bool.orc")

_write("struct<long:boolean>", data_boolean, f"{dir}/long_bool_gzip.orc", pyorc.CompressionKind.ZLIB)

data_dict = {
    "dict": ["abcd", "efgh"] * 32,
}

_write("struct<dict:string>", data_dict, f"{dir}/string_long.orc")

data_dict = {
    "dict": ["abc", "efgh"] * 32,
}

_write("struct<dict:string>", data_dict, f"{dir}/string_dict.orc", dict_key_size_threshold=0.1)

_write("struct<dict:string>", data_dict, f"{dir}/string_dict_gzip.orc", pyorc.CompressionKind.ZLIB)

data_dict = {
    "dict": ["abcd", "efgh"] * (10**4 // 2),
}

_write("struct<dict:string>", data_dict, f"{dir}/string_long_long.orc")
_write("struct<dict:string>", data_dict, f"{dir}/string_long_long_gzip.orc", pyorc.CompressionKind.ZLIB)

long_f32 = {
    "dict": [random.uniform(0, 1) for _ in range(10**6)],
}

_write("struct<dict:float>", long_f32, f"{dir}/f32_long_long_gzip.orc", pyorc.CompressionKind.ZLIB)
