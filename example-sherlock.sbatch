#!/bin/bash
#
#SBATCH --time=00:20:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=4G
#SBATCH --mail-type=ALL
#SBATCH --partition=hns,normal
#SBATCH --job-name="dask-warp-demo"

singularity run docker://ghcr.io/phargogh/prototype-concurrency-in-pygeoprocessing:latest python example-sherlock.py
