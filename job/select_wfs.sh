#!/bin/bash
#PBS -N wfs_{label}
#PBS -q short
#PBS -o /home/shifter/gonzalo/logs/select_wfs_{run_number}_{ldc}.out
#PBS -e /home/shifter/gonzalo/logs/select_wfs_{run_number}_{ldc}.err
#PBS -l mem=2gb

source /data/software/miniconda/etc/profile.d/conda.sh
export ICTDIR=/data/software/IC
export ICDIR=$ICTDIR/invisible_cities
export PATH="$ICTDIR/bin:$PATH"
export PYTHONPATH=$ICTDIR:$PYTHONPATH
export OMP_NUM_THREADS=1
conda activate IC-3.8-2022-04-13

cd $HOME/gonzalo/lsc-utils
source setup.sh

python bin/select_wfs.py -r {run_number} -f {file} -l {ldc} -t 2
