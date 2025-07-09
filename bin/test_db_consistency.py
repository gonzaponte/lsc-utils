from sys      import exit, argv
from pathlib  import Path
from argparse import ArgumentParser

from numpy  import allclose
from pandas import read_hdf

from invisible_cities.database.load_db import DataPMT, DataSiPM

from utils import get_data_path
from utils import select_last_version_city


DEFAULT = "\033[0m"
GREEN   = "\033[92m"
RED     = "\033[91m"
OK      = f"{GREEN}✔✔✔{DEFAULT}"
FAIL    = f"{RED}✘✘✘{DEFAULT}"

COLUMNS_PMT  = "SensorID Active coeff_blr coeff_c adc_to_pes noise_rms Sigma".split()
COLUMNS_SIPM = "SensorID Active adc_to_pes Sigma".split()

parser = ArgumentParser()
parser.add_argument("run", type=int, help="run number")

run = parser.parse_args(argv[1:]).run

datapmt_db  = DataPMT ("next100", run).sort_values("SensorID").filter(COLUMNS_PMT)
datasipm_db = DataSiPM("next100", run).sort_values("SensorID").filter(COLUMNS_SIPM)

path = get_data_path(run) / "prod"
path = select_last_version_city(path, "irene")
path = sorted(path.glob("trigger*"))[0] # first trigger

filenames = [f
             for path in sorted(path.glob("*"))       # each ldc
             for f    in sorted(path.glob("run*.h5")) # each file
             ]

fail = []

ok = False
for filename in sorted(filenames):
    # skip empty files
    if not len(read_hdf(filename, "/Run/events")):
        continue

    datapmt_file  = read_hdf(filename, "/DB/DataPMT" ).filter(COLUMNS_PMT)
    datasipm_file = read_hdf(filename, "/DB/DataSiPM").filter(COLUMNS_SIPM)

    if (not allclose(datapmt_file , datapmt_db ) or
        not allclose(datasipm_file, datasipm_db)):
        print("~"*50)
        print(f"{FAIL} There is no consistency between the data file and the database".center(50))
        print("~"*50)
        exit()

    ok = True
    break


if ok:
    print(f"{OK} Data file and database are consistent")
else:
    print("~"*50)
    print("{FAIL} There is no consistency between the data file and the database".center(50))
    print("~"*50)
