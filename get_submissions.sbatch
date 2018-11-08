#!/bin/bash
#SBATCH --job-name=dl_reddit_corpus
#SBATCH --output=dl_log.txt
#SBATCH --time=48:00:00

ruby get_submssions.rb
