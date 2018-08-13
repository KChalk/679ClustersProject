#!/bin/bash 

module load hadoop

#spark-submit \
#    --master yarn \
#    --num-executors 50 \
#	--executor-memory 6g \
#	filterposts.py
	

#hadoop fs -rm -r l_output.csv
#hadoop fs -rm -r m_output.csv
#hadoop fs -rm -r s_output.csv

spark-submit \
    --master yarn \
    --num-executors 200 \
	--executor-memory 14g \
	getliwc.py
#hadoop fs -getmerge l_output.csv l_output.csv
#hadoop fs -getmerge m_output.csv m_output.csv
#hadoop fs -getmerge s_output_posts.csv s_output.csv
