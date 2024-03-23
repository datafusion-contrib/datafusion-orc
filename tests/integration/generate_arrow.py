# Requires pyarrow to be installed
import glob
from pyarrow import orc, feather

files = glob.glob("data/expected/*")
files = [file.removeprefix("data/expected/").removesuffix(".jsn.gz") for file in files]

ignore_files = [
    "TestOrcFile.testTimestamp" # Root data type isn't struct
]

files = [file for file in files if file not in ignore_files]

for file in files:
    print(f"Converting {file} from ORC to feather")
    table = orc.read_table(f"data/{file}.orc")
    feather.write_feather(table, f"data/expected_arrow/{file}.feather")
