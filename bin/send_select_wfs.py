from sys      import argv
from time     import sleep
from os.path  import expandvars
from pathlib  import Path
from argparse import ArgumentParser

import subprocess as sp

from utils import LDCS


parser = ArgumentParser()
parser.add_argument("-r", "--run-number", type=int)
parser.add_argument("-f", "--events-file", type=Path)

args = parser.parse_args(argv[1:])

template = Path("__file__").parent.parent / "job" / "select_wfs.sh"
template = open(template).read()

jobfile = Path(expandvars("$HOME")) / "gonzalo" / "jobs" / f"select_wfs_{args.run_number}.sh"
open(jobfile, "w").write(template.format(run_number=args.run_number, file=args.events_file.absolute()))

command = f"qsub {jobfile}".split()
process = sp.Popen(command, stdout=sp.PIPE, stderr=sp.PIPE)
while process.poll() is None:
    sleep(.1)

print(process.stdout.read().decode())
print(process.stderr.read().decode())
