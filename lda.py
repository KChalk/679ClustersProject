 
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import size, split
from operator import add
import json 
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, ArrayType, MapType, StringType
from collections import defaultdict
import csv

from pyspark.ml.feature import CountVectorizer
from pyspark.ml.clustering import LDA
import codecs

#from functools import partial
 
def main(): 
    spark = SparkSession \
        .builder \
        .appName("Reddit: LDA") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    size = "medium"  # medium or large
    if size == "large":
        inp = "l_filtered_post_tokens"
        output="l_lda"
    elif size == "medium":
        inp = "m_filtered_post_tokens"
        #file = "file:///g/chalkley/Winter18/679Clusters/Project/m_filtered_posts.csv"
        output="m_lda"
    else:
        inp = "file:///g/chalkley/Winter18/679Clusters/Project/smallestinput"
        output="s_lda"

    sc = spark.sparkContext

    postRDD = spark.read.json(inp+'.json')
    #postRDD = spark.read.parquet(file+'.parquet')

    print('\n\n\n vectorizing... \n\n\n')
    cv=CountVectorizer(inputCol='tokens', outputCol='vectors')
    vecModel=cv.fit(postRDD)

    print('\n\n\n Get Vocab... \n\n\n')
    inv_voc=vecModel.vocabulary 

    f = codecs.open(output+'_vocab.txt', encoding='utf-8', mode='w')
    for item in inv_voc:
        f.write(u'{0}\n'.format(item))

    f.close()


    print('\n\n\n vector transform... \n\n\n')

    withvectors= vecModel.transform(postRDD)

    num_topics=10

    print('\n\n\n LDA... \n\n\n')

    lda=LDA(featuresCol='vectors', k=10, maxIter=50)
    lda_model=lda.fit(withvectors.select('id','vectors'))
    
    print('\n\n\n Describe Topics... \n\n\n')

    topic_indices=lda_model.describeTopics(maxTermsPerTopic=30)
    topic_indices.write.json(output+'_topics.json')


if __name__ == "__main__":
    main()
 