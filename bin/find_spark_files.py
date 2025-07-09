from sys      import argv
from pathlib  import Path
from datetime import datetime
from argparse import ArgumentParser
from pandas   import read_hdf, concat

from utils import get_data_path
from utils import LDCS

def parse_date(s):
    return datetime.strptime(s, "%Y/%m/%d %H:%M:%S")


parser = ArgumentParser()
parser.add_argument("-r", "--run-number", type=int, help="run number")
parser.add_argument("-s", "--start-file", type=int, help="start file", default=0)
parser.add_argument("-d", "--date"      , type=parse_date, help="date as YYYY/MM/DD HH:MM:SS")
parser.add_argument("-w", "--window"    , type=int, help="search window in seconds")

args = parser.parse_args(argv[1:])
path = get_data_path(args.run_number) / "data"

target = args.date.timestamp()
print(f"Target timestamp ({args.date}): {target}")

found = []
for ldc in LDCS:
    filenames = sorted((path / f"ldc{ldc}").glob("*.h5"), reverse=True)
    filemin   = args.start_file if args.start_file else int(0.9*len(filenames)/2) # factor 2 due to double trigger
    print(f"Number of files in ldc {ldc}: {len(filenames)}")
    for filename in filenames:
        fileno = int(filename.stem.split("_")[2])
        if fileno < args.start_file:
            continue
        data = read_hdf(filename, "/Run/events")
        data = data.assign(dt = data.timestamp / 1e3 - target) # factor 1e3 to convert ms to s
        data = data.loc[data.dt.abs() < args.window]
        data = data.assign(filename = filename.stem)
        found.append(data)

found = sorted(found, key=len, reverse=True) # I don't remember the motivation for this
found = concat(found, ignore_index=True)
found = found.sort_values("dt")
found = found.groupby("filename").last()

print(f"Found {len(found)} files that might contain the spark:")
print(found)
