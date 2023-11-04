import shutil
import glob
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()

# TODO: int8, char, varchar, decimal, timestamp, struct, list, map, union
df = spark.createDataFrame(
    [ #   bool,         int16,         int32,         int64,       float32,        float64,              binary,       utf8,             date32
        ( None,          None,          None,          None,          None,           None,                None,       None,               None),
        ( True,             0,             0,             0,           0.0,            0.0,         "".encode(),         "", date(1970,  1,  1)),
        (False,             1,             1,             1,           1.0,            1.0,        "a".encode(),        "a", date(1970,  1,  2)),
        (False,            -1,            -1,            -1,          -1.0,           -1.0,        " ".encode(),        " ", date(1969, 12, 31)),
        ( True, (1 << 15) - 1, (1 << 31) - 1, (1 << 63) - 1,  float("inf"),   float("inf"),   "encode".encode(),   "encode", date(9999, 12, 31)),
        ( True,    -(1 << 15),    -(1 << 31),    -(1 << 63), float("-inf"),  float("-inf"),   "decode".encode(),   "decode", date(1582, 10, 15)),
        ( True,            50,            50,            50,     3.1415927,  3.14159265359, "大熊和奏".encode(), "大熊和奏", date(1582, 10, 16)),
        ( True,            51,            51,            51,    -3.1415927, -3.14159265359, "斉藤朱夏".encode(), "斉藤朱夏", date(2000,  1,  1)),
        ( True,            52,            52,            52,           1.1,            1.1, "鈴原希実".encode(), "鈴原希実", date(3000, 12, 31)),
        (False,            53,            53,            53,          -1.1,           -1.1,       "🤔".encode(),       "🤔", date(1900,  1,  1)),
        ( None,          None,          None,          None,          None,           None,                None,       None,               None),
    ],
    StructType(
        [
            StructField("boolean", BooleanType()),
            StructField(  "int16",   ShortType()),
            StructField(  "int32", IntegerType()),
            StructField(  "int64",    LongType()),
            StructField("float32",   FloatType()),
            StructField("float64",  DoubleType()),
            StructField( "binary",  BinaryType()),
            StructField(   "utf8",  StringType()),
            StructField( "date32",    DateType()),
        ]
    ),
).coalesce(1)

compression = ["none", "snappy", "zlib", "lzo", "zstd", "lz4"]
for c in compression:
    df.write.format("orc")\
      .option("compression", c)\
      .mode("overwrite")\
      .save(f"./alltypes.{c}")
    # Since Spark saves into a directory
    # Move out and rename the expected single ORC file (because of coalesce above)
    orc_file = glob.glob(f"./alltypes.{c}/*.orc")[0]
    shutil.move(orc_file, f"./alltypes.{c}.orc")
    shutil.rmtree(f"./alltypes.{c}")
