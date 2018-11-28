#!/bin/bash 

module load hadoop

spark-submit \
    --master yarn \
    --num-executors 10 \
	testingzone.py

	
