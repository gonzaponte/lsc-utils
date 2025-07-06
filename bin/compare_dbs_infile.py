from sys      import argv
from argparse import ArgumentParser
from pandas   import read_hdf

from utils import get_data_path
from utils import get_last_version_city
from utils import valid_trigger
from utils import valid_ldc

parser = ArgumentParser()
parser.add_argument("-r", "--runs"   , type=int          , help="run numbers to compare", nargs="+")
parser.add_argument("-f", "--fileno" , type=int          , help="file number to use"    , default=0)
parser.add_argument("-t", "--trigger", type=valid_trigger, help="trigger to use"                   )
parser.add_argument("-l", "--ldc"    , type=valid_ldc    , help="ldc to use"                       )

args = parser.parse_args(argv[1:])
if len(runs) < 2:
    raise ValueError("Error: -r/--rows must contain at least two runs")

paths = [get_data_path(run) / "prod"                              for run  in args.runs] # prod     path for each run
paths = [get_last_version_city(path, "irene")                     for path in paths    ] # irene    path
paths = [sorted(path.glob(f"trigger{args.trigger}"))[0]           for path in paths    ] # trigger  path
paths = [sorted(path.glob(f"ldc{args.ldc}"))[0]                   for path in paths    ] # ldc      path
paths = [sorted(path.glob(f"run_*_{args.fileno:>04}_ldc*.h5"))[0] for path in paths    ] # selected file

datapmts  = [pd.read_hdf(path, "/DB/DataPMT" ).sort_values("SensorID") for path in paths]
datasipms = [pd.read_hdf(path, "/DB/DataSiPM").sort_values("SensorID") for path in paths]

for path, datapmt, datasipm in zip(paths, datapmts, datasipms):
    assert datapmt .shape == (  60, 11), f"DataPMT  in {path} has incorrect shape"
    assert datasipm.shape == (3584,  7), f"DataSiPM in {path} has incorrect shape"

path0     = paths    [0]
datapmt0  = datapmts [0]
datasipm0 = datasipms[0]
for path, datapmt, datasipm in zip(paths[1:], datapmt[1:], datasipms[1:]):
    fail = False
    if not np.allclose(datapmt.values, datapmt0.values):
        fail = True
        print("There is a difference between DataPMT in these files:")
        print(path0)
        print(path)
        print("="*80)
        print("DIFF".center(80))
        print("="*80)
        print(f" ID    Active {run0}    Active {run1}    Gain {run0}    Gain {run1}")
        for sid in datapmt0.SensorID.values:
            row0 = datapmt0.loc[lambda db: db.SensorID==sid].iloc[0]
            row1 = datapmt .loc[lambda db: db.SensorID==sid].iloc[0]
            print(f"{sid: ^4}    {row0.Active: ^12}    {row1.Active: ^12}    {row0.adc_to_pes:^12.1f}    {row1.adc_to_pes:^12.1f}")


    if not np.allclose(datasipm.values, datasipm0.values):
        fail = True
        print("There is a difference between DataSiPM in these files:")
        print(path0)
        print(path)
        print("="*80)
        print("DIFF".center(80))
        print("="*80)
        print(f" ID    Active {run0}    Active {run1}    Gain {run0}    Gain {run1}")
        for sid in datasipm0.SensorID.values:
            row0 = datasipm0.loc[lambda db: db.SensorID==sid].iloc[0]
            row1 = datasipm .loc[lambda db: db.SensorID==sid].iloc[0]
            print(f"{sid: ^4}    {row0.Active: ^12}    {row1.Active: ^12}    {row0.adc_to_pes:^12.1f}    {row1.adc_to_pes:^12.1f}")

    if fail:
        break
