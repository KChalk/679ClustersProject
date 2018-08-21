
'''
todo:
address memory problems
save smaller dataset
improve splitting and prefix matching
'''
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import size, split
from operator import add
import json
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, ArrayType, MapType, StringType
from collections import defaultdict
import csv
import re

  
def main():
	spark = SparkSession \
		.builder \
		.appName("Reddit:Filter posts") \
		.config("spark.some.config.option", "some-value") \
		.getOrCreate()

	size = "medium"  # medium or large
	if size == "large":
		file = "RS_full_corpus.bz2"
		output="l_filtered_post_tokens"
	elif size == "medium":
		file = "RS_2017_11.bz2"
		output="m_filtered_post_tokens"
	else:
		file = "file:///g/chalkley/Winter18/679Clusters/Project/redditexcerpt.txt"
		output="s_filtered_post_tokens"

	sc = spark.sparkContext
# filter
#	postRDD = filterPosts(file, sc, spark)
	print('\n\n\n starting read and filter')
	filtered = filterPostsAllSubs(file, sc, spark)
	tokens = filtered.select('tokens')

	tokenRDD=tokens.rdd

	print('\n\n\n getting vocabulary')
	vocab=sc.broadcast(getVocab(tokenRDD,sc,spark))

	print('\n\n\n vectorizing... ')
	print(filtered.select('id','tokens').rdd.first())

	## Save posts
#	filtered.write.parquet(output, mode='overwrite')
#	filtered.write.json(output+'.json', mode='overwrite')

def document_vector(inputRDD):
	#modified from https://sean.lane.sh/blog/2016/PySpark_and_LDA
    counts = defaultdict(int)
    for token in wordlist:
        if token in vocab:
            token_id = vocab[token]
            counts[token_id] += 1
    counts = sorted(counts.items())
    keys = [x[0] for x in counts] 
    values = [x[1] for x in counts]
    return (Vectors.sparse(len(vocab), keys, values))
					
def splitlen(s):
	tokens=s.split()
	return len(tokens)
 
def tokenize(s):
	s=s.strip().lower()
	wordlist=re.split("[\s;,#]", s)
	#the code below elimates any word containing an apostrophe or other punctuation
	#for word in wordlist: 
	#	if word.isalpha():
	#		tokens.append(word)
	return wordlist 

def filterPostsAllSubs(filename, sc, ss):
	
	#splitlenUDF = udf(splitlen, IntegerType()) 
	tokensUDF = udf(tokenize, ArrayType(StringType())) 
	
	alldata = ss.read.json(filename)

	#remove links to external content from data
	longselfposts = alldata		\
		.filter(alldata['is_self'] == True) 	\
		.select('id','subreddit',tokensUDF('selftext').alias("tokens"))	\
		.withColumn('wordcount', size('tokens'))	\
		.filter('wordcount >= 100') 

	return longselfposts


def getVocab(inputRDD, sc, ss):
	#inspired by https://sean.lane.sh/blog/2016/PySpark_and_LDA
	# I dont think all of the following steps are necessary, as Im not filtering out the most common words, but Im keeping it as a method of turning words into word ids

	vocab= inputRDD 	\
		.flatMap(lambda text :  text)	\
		.map(lambda word: (word,1) ) 	\
		.reduceByKey(lambda word, value: word + value)\
		.map(lambda x: x[0])		\
		.zipWithIndex()			
	print('\n\n\n foobar \n\n\n')
	vocab= vocab	  \
		.collectAsMap()
	return vocab


	#fails
	#18/08/15 15:24:12 WARN TaskSetManager: Lost task 48.1 in stage 1.0 ...
	# ... line 177, in main process()
  	# ... line 172, in process serializer.dump_stream(func(split_index, iterator), outfile)
  	# ... pyspark/rdd.py", line 2423, in pipeline_func
  	# ... pyspark/rdd.py", line 2423, in pipeline_func
  	# ... pyspark/rdd.py", line 346, in func
  	# ... pyspark/rdd.py", line 1842, in combineLocally
  	# ... pyspark/shuffle.py", line 238, in mergeValues
    # d[k] = comb(d[k], v) if k in d else creator(v)
	# TypeError: unhashable type: 'list'

	#likely differences between this and web example due to conversion from DF to RDD after filter and select
'''
	vocab= inputRDD 	\
		.flatMap(lambda text :  text)	\
		.map(lambda word: (word,1) ) 	\
		.reduceByKey(lambda word, value: word + value)  \
		.map(lambda tuple: (tuple[1], tuple[0]))	\
		.sortByKey(False)	### ERROR IN THIS LINE!!!!!
		.map(lambda x: x[1])		\
		.zipWithIndex()			\
		.collectAsMap()
	return vocab
'''


if __name__ == "__main__":
	main()
 