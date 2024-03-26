from datetime import datetime as dttm
import pyarrow as pa
from pyarrow import orc

schema = pa.schema([
    pa.field('timestamp1', pa.timestamp("ns")),
    pa.field('timestamp2', pa.timestamp("ns", tz="UTC")),
])
c1 = pa.array([None, dttm(1970, 1, 1), dttm(1970, 1, 2, 23, 59, 59), dttm(1969, 12, 31, 23, 59, 59), dttm(2262, 4, 11, 11, 47, 16), dttm(2001, 4, 13), dttm(2000, 1, 1, 23, 10, 10), dttm(1900, 1, 1)])
c2 = pa.array([None, dttm(1970, 1, 1), dttm(1970, 1, 2, 23, 59, 59), dttm(1969, 12, 31, 23, 59, 59), dttm(2262, 4, 11, 11, 47, 16), dttm(2001, 4, 13), dttm(2000, 1, 1, 23, 10, 10), dttm(1900, 1, 1)])
table = pa.Table.from_arrays([c1, c2], schema=schema)
orc.write_table(table, "pyarrow_timestamps.orc")

