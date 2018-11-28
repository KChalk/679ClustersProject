#!/bin/bash 

module load hadoop

#export PYSPARK_PYTHON=python3 

#spark-submit \
#    --master yarn \
#    --num-executors 200 \
#	--executor-memory 14g \
#	filterposts.py

#spark-submit \
#    --master yarn \
#    --num-executors 200 \
#	--executor-memory 14g \
#	lda.py

#hadoop fs -getmerge l_lda_topics.json l_lda_topics.json

#hadoop fs -rm -r l_output.csv
#hadoop fs -rm -r m_output.csv
#hadoop fs -rm -r s_output.csv

#spark-submit \
#    --master yarn \
#    --num-executors 200 \
#	--executor-memory 14g \
#	getabs.py

spark-submit \
   --master yarn \
   --num-executors 100 \
	--executor-memory 6g \
        cluster.py

#spark-submit \
#    --master yarn \
#    --num-executors 200 \
#        --executor-memory 14g \
#        getliwc.py

#hadoop fs -getmerge l_output.csv l_output.csv
#hadoop fs -getmerge m_output.csv m_output.csv
#hadoop fs -getmerge s_output_posts.csv s_output.csv
