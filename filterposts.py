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
	
	## Save posts
	filtered.write.parquet(output+'.parquet', mode='overwrite')
	#withvectors.write.json(output+'.json', mode='overwrite')

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

if __name__ == "__main__":
	main()
 

 
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


	## Save posts
#	filtered.write.parquet(output, mode='overwrite')
	filtered.write.json(output+'.json', mode='overwrite')

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

if __name__ == "__main__":
	main()
 