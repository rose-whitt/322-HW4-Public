#!/bin/bash

#SBATCH --job-name=comp322-hw4
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=16000m
#SBATCH --time=01:00:00
#SBATCH --mail-type=ALL
#SBATCH --export=ALL
#SBATCH --partition=commons
#SBATCH --exclusive
 
cd /home/$USER/hw4-rose-whitt # TODO: Change path to your hw 4 folder

source /home/mjoyner/comp322/322_setup.sh

mvn -DBoruvkaPerformanceTest test
