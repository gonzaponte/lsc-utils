from sys      import argv, stderr, exit
from pathlib  import Path
from shutil   import copy
from argparse import ArgumentParser

import numpy  as np
import tables as tb
import pandas as pd

from invisible_cities.io      .dst_io   import df_writer
from invisible_cities.io      .pmaps_io import load_pmaps_as_df
from invisible_cities.cities  .irene    import store_db_info
from invisible_cities.database.load_db  import DataPMT, DataSiPM


def recalibrate(path_in, path_out, run):
    datapmt_bad  = pd.read_hdf(path_in, "/DB/DataPMT" ).set_index("SensorID")
    datasipm_bad = pd.read_hdf(path_in, "/DB/DataSiPM")

    datapmt_ok  = DataPMT ("next100", run).loc[lambda df: df.Active==1].set_index("SensorID")
    datasipm_ok = DataSiPM("next100", run)

    copy(path_in, path_out)
    s1, s2, si, s1_pmt, s2_pmt = load_pmaps_as_df(path_in)

    with tb.open_file(path_out, "r+") as file_out:
        file_out.remove_node(file_out.root.DB, recursive=True)
        store_db_info(file_out, "next100", run)

        # S1
        n = s1_pmt.groupby("event peak npmt".split()).ene.count().values
        n = [np.arange(ni) for ni in n]
        n = np.concatenate(n)
        s1_pmt.insert(s1_pmt.shape[1], "timebin", n)

        factors     = ( datapmt_bad.adc_to_pes.loc[s1_pmt.npmt.values].values
                      / datapmt_ok .adc_to_pes.loc[s1_pmt.npmt.values].values)
        s1_pmt.ene *= factors
        s1sum       = s1_pmt.groupby("event peak timebin".split()).ene.sum().values

        # S2 - PMT
        n = s2_pmt.groupby("event peak npmt".split()).ene.count().values
        n = [np.arange(ni) for ni in n]
        n = np.concatenate(n)
        s2_pmt.insert(s2_pmt.shape[1], "timebin", n)

        factors     = ( datapmt_bad.adc_to_pes.loc[s2_pmt.npmt.values].values
                      / datapmt_ok .adc_to_pes.loc[s2_pmt.npmt.values].values)
        s2_pmt.ene *= factors
        s2sum       = s2_pmt.groupby("event peak timebin".split()).ene.sum().values

        # S2 - SiPM
        factors  = ( datasipm_bad.adc_to_pes.loc[si.nsipm.values].values
                   / datasipm_ok .adc_to_pes.loc[si.nsipm.values].values)
        si.ene  *= factors

        file_out.root.PMAPS.S1   .cols.ene[:] = s1sum
        file_out.root.PMAPS.S2   .cols.ene[:] = s2sum
        file_out.root.PMAPS.S2Si .cols.ene[:] = si    .ene.values
        file_out.root.PMAPS.S1Pmt.cols.ene[:] = s1_pmt.ene.values
        file_out.root.PMAPS.S2Pmt.cols.ene[:] = s2_pmt.ene.values


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-i",  "--input-files" , type=Path, required=True, nargs="+")
    parser.add_argument("-o",  "--output-files", type=Path, required=True, nargs="+")

    args      = parser.parse_args(argv[1:])
    paths_in  = args. input_files
    paths_out = args.output_files

    assert len(paths_in) == len(paths_out), "Number of input and output files must be the same"

    nerr = 0
    for path_in, path_out in zip(paths_in, paths_out):
        run = int(path_in.stem.split("_")[1])

        try:
            recalibrate(path_in, path_out, run)
        except Exception as e:
            nerr += 1
            print(f"An error occurred when processing file {path_in}", file=stderr)
            print(str(e), file=stderr)
            if nerr > 20:
                print("Exiting due to too many failures")
                exit()
