#!/bin/bash 
hadoop fs -rm -r bigoutput.csv
module load hadoop

spark-submit \
    --master yarn \
    --num-executors 100 \
	--executor-memory 5g \
	3main.py
	
hadoop fs -getmerge bigoutput.csv bigoutput.csv
