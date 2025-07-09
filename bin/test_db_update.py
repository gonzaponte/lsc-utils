from sys import argv
from argparse import ArgumentParser

from pandas import DataFrame

from invisible_cities.database.load_db import DataPMT, DataSiPM


parser = ArgumentParser()
parser.add_argument("new_min_run", type=int, help="run number for which there are new calib constants")

run = parser.parse_args(argv[1:]).new_min_run

pmts_before  = DataPMT ("next100", run - 1).sort_values("SensorID").set_index("SensorID").drop(columns="ChannelID X Y Sigma PmtID noise_rms".split())
pmts_after   = DataPMT ("next100", run    ).sort_values("SensorID").set_index("SensorID").drop(columns="ChannelID X Y Sigma PmtID noise_rms".split())
sipms_before = DataSiPM("next100", run - 1).sort_values("SensorID").set_index("SensorID").drop(columns="ChannelID X Y Sigma".split())
sipms_after  = DataSiPM("next100", run    ).sort_values("SensorID").set_index("SensorID").drop(columns="ChannelID X Y Sigma".split())

pmts_joint  = DataFrame.join( pmts_before,  pmts_after, lsuffix="_before", rsuffix="_after")
sipms_joint = DataFrame.join(sipms_before, sipms_after, lsuffix="_before", rsuffix="_after")

pmts_joint .insert( pmts_joint.shape[1], "gain_diff"      ,  pmts_joint.adc_to_pes_after -  pmts_joint.adc_to_pes_before)
sipms_joint.insert(sipms_joint.shape[1], "gain_diff"      , sipms_joint.adc_to_pes_after - sipms_joint.adc_to_pes_before)
pmts_joint .insert( pmts_joint.shape[1], "updated_masking",  pmts_joint.Active_after     !=  pmts_joint.Active_before)
sipms_joint.insert(sipms_joint.shape[1], "updated_masking", sipms_joint.Active_after     != sipms_joint.Active_before)
pmts_joint .insert( pmts_joint.shape[1], "coeff_c_diff"   ,  pmts_joint.coeff_c_after    -  pmts_joint.coeff_c_before)
pmts_joint .insert( pmts_joint.shape[1], "coeff_blr_diff" ,  pmts_joint.coeff_blr_after  -  pmts_joint.coeff_blr_before)

updated_pmts  =  pmts_joint.loc[( pmts_joint.gain_diff.abs() > 1e-3) | ( pmts_joint.updated_masking) | (pmts_joint.coeff_c_diff.abs() > 1e-9) | (pmts_joint.coeff_blr_diff.abs() > 1e-6)]
updated_sipms = sipms_joint.loc[(sipms_joint.gain_diff.abs() > 1e-3) | (sipms_joint.updated_masking)]

if len(updated_pmts):
    print("Updated PMTs:")
    print(updated_pmts)
else:
    print("!"*80)
    print(f"No PMTs were updated between runs {run-1} and {run}")
    print("!"*80)
    print()

if len(updated_sipms):
    print("Updated SiPMs:")
    print(updated_sipms)
else:
    print("!"*80)
    print(f"No SiPMs were updated between runs {run-1} and {run}")
    print("!"*80)
    print()
