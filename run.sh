#!/bin/bash 
module load hadoop

spark-submit \
    --master yarn \
    --num-executors 50 \
	main.py