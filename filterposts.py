'''
todo:'''
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

 
def main():
	spark = SparkSession \
		.builder \
		.appName("Reddit:Filter posts") \
		.config("spark.some.config.option", "some-value") \
		.getOrCreate()

	size = "large"  # medium or large
	if size == "large":
		file = "RS_full_corpus.bz2"
		output="l_filtered_posts.csv"
	elif size == "medium":
		file = "RS_2017_11.bz2"
		output="m_filtered_posts.csv"
	else:
		file = "file:///g/chalkley/Winter18/679Clusters/Project/redditexcerpt.txt"
		output="s_filtered_posts.csv"

	sc = spark.sparkContext
# filter
#	postRDD = filterPosts(file, sc, spark)
	print('\n\n\n starting read and filter')
	postRDD = filterPostsAllSubs(file, sc, spark)
	## Save post RDD
	postRDD.write.parquet(output, mode='overwrite')


					
def splitlen(s):
	tokens=s.split()
	return len(tokens)
 
def mysplit(s):
	tokens=s.split()
	return tokens

def filterPosts(filename, sc, ss):
	allposts = ss.read.json(filename)
	print('\n\n\n')
	print('all posts num partiations', allposts.rdd.getNumPartitions())
	subreddits=set(['depression', 'Anxiety', 'SuicideWatch','HomeImprovement','tipofmytongue','dogs','jobs','r4r','electronic_cigarette','asktrp','keto','trees','relationships','asktransgender','askgaybros','Advice','relationship_advice','CasualConversation','Drugs','teenagers','MGTOW','TwoXChromosomes','Asthma', 'diabetes', 'cancer','reddit.com','parenting', 'raisedbynarcissists','stopdrinking','ADHD'])
	print('\n\n\n')

	#select posts from desired subreddits and remove links to external content
	subRposts = allposts.filter(allposts['subreddit'].isin(subreddits) & allposts['is_self'] == True)
	
	splitlenUDF = udf(splitlen, IntegerType())

	#select desired columns and create wordcount based on simple whitespace splitting
	bettercols=subRposts.select('id','subreddit','selftext',splitlenUDF('selftext').alias("wordcount"))

	#remove posts of less than 100 words
	longposts = bettercols.filter(bettercols['wordcount'] >= 100) 
	return longposts


def filterPostsAllSubs(filename, sc, ss):
	allposts = ss.read.json(filename)
	print('\n\n\n')
	print('all posts num partiations', allposts.rdd.getNumPartitions())
	print('\n\n\n')

	#remove links to external content from data
	subRposts = allposts.filter(allposts['is_self'] == True) 
	splitlenUDF = udf(splitlen, IntegerType()) 
	
	#select desired columns and create wordcount based on simple whitespace splitting
	bettercols=subRposts.select('id','subreddit','selftext',splitlenUDF('selftext').alias("wordcount"))
	
	#remove posts of less and 100 words
	longposts = bettercols.filter(bettercols['wordcount'] >= 100) 

	return longposts
	
if __name__ == "__main__":
	main()
 

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

 
def main():
	spark = SparkSession \
		.builder \
		.appName("Reddit:Filter posts") \
		.config("spark.some.config.option", "some-value") \
		.getOrCreate()

	size = "medium"  # medium or large
	if size == "large":
		file = "RS_full_corpus.bz2"
		output="l_filtered_posts.csv"
	elif size == "medium":
		file = "RS_2017_11.bz2"
		output="m_filtered_posts.csv"
	else:
		file = "file:///g/chalkley/Winter18/679Clusters/Project/redditexcerpt.txt"
		output="s_filtered_posts.csv"

	sc = spark.sparkContext
# filter
#	postRDD = filterPosts(file, sc, spark)
	print('\n\n\n starting read and filter')
	postRDD = filterPostsAllSubs(file, sc, spark)
	## Save post RDD
	postRDD.write.csv(output, header=True)


					
def splitlen(s):
	tokens=s.split()
	return len(tokens)
 
def mysplit(s):
	tokens=s.split()
	return tokens

def filterPosts(filename, sc, ss):
	allposts = ss.read.json(filename)
	print('\n\n\n')
	print('all posts num partiations', allposts.rdd.getNumPartitions())
	subreddits=set(['depression', 'Anxiety', 'SuicideWatch','HomeImprovement','tipofmytongue','dogs','jobs','r4r','electronic_cigarette','asktrp','keto','trees','relationships','asktransgender','askgaybros','Advice','relationship_advice','CasualConversation','Drugs','teenagers','MGTOW','TwoXChromosomes','Asthma', 'diabetes', 'cancer','reddit.com','parenting', 'raisedbynarcissists','stopdrinking','ADHD'])
	print('\n\n\n')

	#select posts from desired subreddits and remove links to external content
	subRposts = allposts.filter(allposts['subreddit'].isin(subreddits) & allposts['is_self'] == True)
	
	splitlenUDF = udf(splitlen, IntegerType())

	#select desired columns and create wordcount based on simple whitespace splitting
	bettercols=subRposts.select('id','subreddit','selftext',splitlenUDF('selftext').alias("wordcount"))

	#remove posts of less than 100 words
	longposts = bettercols.filter(bettercols['wordcount'] >= 100) 
	return longposts


def filterPostsAllSubs(filename, sc, ss):
	allposts = ss.read.json(filename)
	print('\n\n\n')
	print('all posts num partiations', allposts.rdd.getNumPartitions())
	print('\n\n\n')

	#remove links to external content from data
	subRposts = allposts.filter(allposts['is_self'] == True) 
	splitlenUDF = udf(splitlen, IntegerType()) 
	
	#select desired columns and create wordcount based on simple whitespace splitting
	bettercols=subRposts.select('id','subreddit','selftext',splitlenUDF('selftext').alias("wordcount"))
	
	#remove posts of less and 100 words
	longposts = bettercols.filter(bettercols['wordcount'] >= 100) 

	return longposts
	
if __name__ == "__main__":
	main()
 
