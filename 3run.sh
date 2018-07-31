#!/bin/bash 
# hadoop fs -rm -r l_filtered_posts.csv
# hadoop fs -rm -r m_filtered_posts.csv
 hadoop fs -rm -r s_filtered_posts.csv

module load hadoop

spark-submit \
    --master yarn \
    --num-executors 100 \
	--executor-memory 6g \
	filterposts.py
	
#hadoop fs -getmerge l_filtered_posts.csv l_filtered_posts.csv
#hadoop fs -getmerge m_filtered_posts.csv m_filtered_posts.csv
hadoop fs -getmerge s_filtered_posts.csv s_filtered_posts.csv
