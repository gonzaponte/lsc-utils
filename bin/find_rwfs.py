from pathlib  import Path
from argparse import ArgumentParser


parser = ArgumentParser()
parser.add_argument("-min", "--min-run", type=int      , help="Min run to consider", default=0)
parser.add_argument("-max", "--max-run", type=int      , help="Max run to consider", default=10**5)
parser.add_argument("-l", "--ldc"      , type=valid_ldc, help="LDCs to search"     , default=(1,))

args = parser.parse_args(argv[1:])
ldcs = LDCS if "*" in args.ldc else args.ldc

analysis = Path("/analysis/")

for path in sorted(analysis.glob("*"), reverse=True):
    run = int(path.name)
    if not (args.min_run <= run <= args.max_run): continue

    path = path / "hdf5/data"
    if not path.exists(): continue

    nfiles = 0
    for ldc in ldcs:
        ldcpath = path / f"ldc{ldc}"
        if ldcpath.exists():
            nfiles += sum(1 for _ in ldcpath.glob("run*h5"))
    print(f"Run {run} has {nfiles} files in LDCs {ldcs}")
