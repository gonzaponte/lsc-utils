#!/usr/bin/env python3

from sys      import argv
from pathlib  import Path
from time     import time
from argparse import ArgumentParser

from tables import open_file, Filters, Int64Col, StringCol, IsDescription
from pandas import read_hdf, concat
from numpy  import loadtxt

from invisible_cities.io.run_and_event_io import run_and_event_writer
from invisible_cities.io.dst_io           import df_writer

from utils import LDCS
from utils import TRIGGERS
from utils import get_data_path
from utils import find_city_path
from utils import valid_ldc
from utils import valid_trigger


parser = ArgumentParser()
parser.add_argument("-r", "--run-number"   , type=int          , help="run number"                  , required=True)
parser.add_argument("-f", "--events-file"  , type=Path         , help="events file"                 , required=True)
parser.add_argument("-i", "--initial-index", type=int          , help="start index for output files", default=0)
parser.add_argument("-s", "--start-file"   , type=int          , help="first file number"           , default=0)
parser.add_argument("-e", "--end-file"     , type=int          , help="last  file number"           , default=10**5)
parser.add_argument("-t", "--trigger"      , type=valid_trigger, help="trigger"                     , default="*")
parser.add_argument("-l", "--ldc"          , type=valid_ldc    , help="ldc to scan"                 , required=True)

args   = parser.parse_args(argv[1:])
path   = get_data_path(args.run_number)
output = path / "selected" / "hits"
path   = find_city_path(path / "prod", "sophronia", True)
if not output.exists():
    output.mkdir(parents=True, exist_ok=True)

NEVT_PER_FILE = 10000
ALL_LDCS      = args.ldc == "*"

suffix = args.events_file.suffix
if   suffix == ".h5" : events_to_store = read_hdf(args.events_file, "/RUN/Selected_events").event.drop_duplicates().values
elif suffix == ".txt": events_to_store =  loadtxt(args.events_file).flatten()
else                 : raise ValueError(f"Invalid file extension {suffix}. Valid options: .h5, .txt")

_nevt           = NEVT_PER_FILE      - 1
_nfile          = args.initial_index - 1
_current        = None
_current_ldc    = None
_buffer_size    = None
_write_hits     = None
_write_run      = None
_write_evtmap   = None

class EvtMap(IsDescription):
    evt_number =  Int64Col(shape=(), pos=0)
    file_no    =  Int64Col(shape=(), pos=1)
    ldc        =  Int64Col(shape=(), pos=2)
    filename   = StringCol(     100, pos=3)

def evtmap_writer(file):
    table = file.create_table(file.root, "evtmap", EvtMap, "event mapping")
    def write(evt, filename):
        _, _, fileno, ldc, _ = filename.split("_")
        ldc = int(ldc[-1])
        row = table.row
        row["evt_number"] = evt
        row["file_no"   ] = fileno
        row["ldc"       ] = ldc
        row["filename"  ] = filename
        row.append()
    return write

def get_writers():
    global _nevt, _nfile, _current, _current_ldc, _write_hits, _write_run, _write_evtmap
    _nevt += 1
    if _nevt == NEVT_PER_FILE:
        _nfile       += 1
        _nevt         = 0
        _current_ldc  = ldc
        filename      = (f"selected_hits_{args.run_number}_{_nfile:04}.h5"                if ALL_LDCS else
                         f"selected_hits_{args.run_number}_ldc_{args.ldc}_{_nfile:04}.h5" )
        filename      = output / filename
        if _current:
            _current.flush()
            _current.close()

        _current        = open_file(filename, "w", filters=Filters(complib="zlib", complevel=4))
        _write_hits     = lambda df: df_writer(_current, df, "RECO", "Events")
        _write_run      = run_and_event_writer(_current)
        _write_evtmap   = evtmap_writer(_current)
    return ( _write_hits
           , _write_run
           , _write_evtmap
           )

def store_event(filename, i, hits):
    write_hits, write_evt, write_evtmap = get_writers()
    with open_file(filename) as file:
        evt, ts = file.root.Run.events[i]
        write_hits  (hits)
        write_evt   (args.run_number, evt, ts)
        write_evtmap(evt, filename.stem)

ldcs = LDCS if ALL_LDCS else (args.ldc,)
trgs = TRIGGERS if args.trigger == "*" else (args.trigger,)
t0   = time()
for trigger in trgs:
    for ldc in ldcs:
        ldc_path  = path / f"trigger{trigger}" / f"ldc{ldc}"
        filenames = sorted(ldc_path.glob(f"run_{args.run_number}_*_ldc{ldc}_trg{args.trigger}.*.h5"))
        for filename in filenames:
            fileno = int(filename.stem.split("_")[2])
            if not (args.start_file <= fileno < args.end_file): continue
            print(f"\rLDC {ldc} FILE {fileno:04}", end="", flush=True)

            try:
                events_in_file = read_hdf(filename, "/Run/events").evt_number.values
                hits           = read_hdf(filename, "/RECO/Events")
            except:
                continue
            for i, event in enumerate(events_in_file):
                if event not in events_to_store: continue
                store_event(filename, i, hits.loc[hits.event==event])

_current.flush()
_current.close()
total_time = time() - t0
print(f"\rFINISHED in {total_time:.0f} seconds")
