from sys      import argv, exit
from pathlib  import Path
from argparse import ArgumentParser
from tables   import open_file

from utils import LDCS
from utils import valid_ldc
from utils import valid_trigger
from utils import get_data_path
from utils import find_city_path


parser = argparse.ArgumentParser()
parser.add_argument("run_number"       , type=int           )
parser.add_argument("-ldc", "--ldc"    , type=valid_ldc     , default="*", nargs="+")
parser.add_argument("-t"  , "--trigger", type=valid_trigger , default="*")
parser.add_argument("--rwf"            , action="store_true")
parser.add_argument("--pmaps"          , action="store_true")
parser.add_argument("--hits"           , action="store_true")
parser.add_argument("--last-version"   , action="store_true")

args = parser.parse_args(argv[1:])

if not any((args.rwf, args.pmaps, args.hits)):
    print("At least one of --rwf --pmaps and --hits must be passed!")
    print("-"*80)
    parser.print_help()
    exit(1)

run  = args.run_number
path = get_data_path(run)
ldcs = LDCS if "*" in args.ldc else args.ldc


def count(path, label):
    ntot = 0
    for ldc in ldcs:
        nldc = 0
        ldcpath = path / f"ldc{ldc}"
        for filename in ldcpath.glob("*.h5"):
            with open_file(filename) as file:
                nldc += file.root.Run.events.shape[0]
        print(f"# events ({label}) on LDC {ldc}: {nldc}")
        ntot += nldc
    print(f"# events ({label}) in total: {ntot}")


if args.rwf:
    path_rwf = path / "data"
    count(path_rwf, "waveforms")

if args.pmaps:
    path_pmaps = find_city_path(path / "prod", "irene", args.last_version)
    count(path, "pmaps")

if args.hits:
    hits_path = find_city_path(path / "prod", "sophronia", args.last_version)
    count(hits_path, "hits")
