from datetime import datetime as dttm
import pyarrow as pa
from pyarrow import orc
from pyarrow import parquet
import pyorc

schema = pa.schema([
    pa.field('timestamp_notz', pa.timestamp("ns")),
    pa.field('timestamp_utc', pa.timestamp("ns", tz="UTC")),
])

# TODO test with other non-UTC timezones
arr = pa.array([
    None,
    dttm(1970,  1,  1,  0,  0,  0),
    dttm(1970,  1,  2, 23, 59, 59),
    dttm(1969, 12, 31, 23, 59, 59),
    dttm(2262,  4, 11, 11, 47, 16),
    dttm(2001,  4, 13,  2, 14,  0),
    dttm(2000,  1,  1, 23, 10, 10),
    dttm(1900,  1,  1, 14, 25, 14),
])
table = pa.Table.from_arrays([arr, arr], schema=schema)
orc.write_table(table, "pyarrow_timestamps.orc")


# pyarrow overflows when trying to write this, so we have to use pyorc instead
class TimestampConverter:
    @staticmethod
    def from_orc(obj, tz):
        return obj
    @staticmethod
    def to_orc(obj, tz):
        return obj
schema = pyorc.Struct(
    id=pyorc.Int(),
    timestamp=pyorc.Timestamp()
)
with open("overflowing_timestamps.orc", "wb") as f:
    with pyorc.Writer(
        f,
        schema,
        converters={pyorc.TypeKind.TIMESTAMP: TimestampConverter},
    ) as writer:
        writer.write((1, (12345678, 0)))
        writer.write((2, (-62135596800, 0)))
        writer.write((3, (12345678, 0)))
