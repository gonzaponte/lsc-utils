from sys      import argv
from pathlib  import Path
from argparse import ArgumentParser

from tables import open_file
from pandas import concat

from utils import LDCS
from utils import valid_ldc
from utils import get_data_path
from utils import find_city_path

parser = ArgumentParser()
parser.add_argument("run_number", type=int)
parser.add_argument("-o"  , "--output-file" , type=Path     )
parser.add_argument("-ldc", "--ldc"         , type=valid_ldc, nargs="+", default="*")
parser.add_argument("-t"  , "--trigger"     , type=valid_trigger       , default="*")
parser.add_argument("--rwf"          , action="store_true")
parser.add_argument("--pmaps"        , action="store_true")
parser.add_argument("--hits"         , action="store_true")
parser.add_argument("--last-version" , action="store_true")

args = parser.parse_args(argv[1:])
ldcs = LDCS if "*" in args.ldc else args.ldc

if not any((args.rwf, args.pmaps, args.hits)):
    print("At least one of --rwf --pmaps and --hits must be passed!")
    print("-"*80)
    parser.print_help()
    exit(1)

run    = args.run_number
path   = get_data_path(run)
output = args.output_file

def stats(path, trigger):
    st = []
    if sum(1 for _ in path.glob("trigger*")): # pmaps, hits
        paths = [path / "trigger0", path / "trigger1", path / "trigger2"]
        if trigger != "*":
            paths = paths[int(trigger)]
        paths = list(filter(Path.exists, paths))
    else: # rwf
        paths = [path]

    for path in paths:
        for ldc in args.ldc:
            ldcpath = path / f"ldc{ldc}"
            for filename in ldcpath.glob("run*.h5"):
                st.append(pd.read_hdf(filename, "/Run/events"))
    return concat(st, ignore_index=True)


with open_file(output, "w"):
    pass # simply overwrite file

if args.rwf:
    path_rwf = path / "data"
    st = stats(path_rwf, "waveforms", args.trigger)
    st.to_hdf(output, "/rwf", complib="zlib", complevel=4, mode="a")

if args.pmaps:
    path_pmaps = find_city_path(path / "prod", "irene", args.last_version)
    st = stats(path_pmaps, "pmaps", args.trigger)
    st.to_hdf(output, "/pmaps", complib="zlib", complevel=4, mode="a")

if args.hits:
    path_hits = find_city_path(path / "prod", "sophronia", args.last_version)
    st = stats(path_hits, "hits", args.trigger)
    st.to_hdf(output, "/hits", complib="zlib", complevel=4, mode="a")
