from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import size, split
from operator import add
import json
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, ArrayType, MapType, StringType
from collections import defaultdict


def main():
	spark = SparkSession \
		.builder \
		.appName("Reddit:ETA<20min") \
		.config("spark.some.config.option", "some-value") \
		.getOrCreate()


	size = "large"  # medium or large
	if size == "large":
		file = "RS_full_corpus.bz2"
	elif size == "medium":
		file = "RS_2017_11.bz2"
	else:
		file = "file:///g/chalkley/Winter18/679Clusters/Project/redditexcerpt.txt"

	sc = spark.sparkContext

# filter
	postRDD = filterPosts(file, sc, spark)
	
# freq
	abscounts = calculatePosts(postRDD, sc, spark)
	# sorted=abscounts.rdd.takeOrdered(100,key=lambda x: -x['count(1)'])
# count
	# subRs=countPosts(postRDD,sc,spark)

# collect
	#collected = sorted

	collected = abscounts.collect()

#print tabular
	with open('output.tsv','w') as output:
		output.write('subreddit\t posts \t wordcount \t abscount \t freq\n')
		for row in collected:
			try:
				output.write(row['subreddit']+'\t' +str(row['count(1)'])+ '\t' + str(row['sum(wordcount)']) + '\t'+ str(row['sum(abscount)']) +'\t'+ str(row['absfreq'])+'\n')
			except TypeError:
				output.write('TypeError\n')
def splitlen(s):
	tokens=s.split()
	return len(tokens)
 
def mysplit(s):
	tokens=s.split()
	return tokens

def filterPosts(filename, sc, ss):
	allposts = ss.read.json(filename)
	subreddits=set(['depression', 'Anxiety', 'SuicideWatch','HomeImprovement','tipofmytongue','dogs','jobs','r4r','electronic_cigarette','asktrp','keto','trees','relationships','asktransgender','askgaybros','Advice','relationship_advice','CasualConversation','Drugs','teenagers','MGTOW','TwoXChromosomes','Asthma', 'diabetes', 'cancer','reddit.com','parenting', 'raisedbynarcissists','stopdrinking','ADHD'])
	
#	subRposts = allposts.filter(allposts['subreddit'].isin(subreddits) & allposts['is_self'] == True)
	
	subRposts = allposts.filter(allposts['is_self'] == True)
	splitlenUDF = udf(splitlen, IntegerType())
	bettercols=subRposts.select('id','subreddit','selftext',splitlenUDF('selftext').alias("wordcount"))
	longposts = bettercols.filter(bettercols['wordcount'] >= 100) 
	return longposts

def getabs(text):
	abscount =0
	words=text.split()
	absolutist = set(['absolutely', 'all', 'always', 'complete', 'competely','constant', 'constantly', 'definitely', 'entire', 'ever', 'every', 'everyone', 'everything', 'full', 'must', 'never', 'nothing', 'totally','whole'])
	for word in words:
		if word.lower() in absolutist:
			abscount+=1 
	return abscount


def calculatePosts(posts, sc, ss):
	getabsUDF = udf(getabs, IntegerType())

	abscount = posts.withColumn("abscount", getabsUDF('selftext'))

	grouped = abscount.groupBy(abscount['subreddit'])
	counts = grouped.agg({"*": "count", "wordcount": "sum", "abscount": "sum"})
	final = counts.withColumn("absfreq", counts['sum(abscount)'] / counts['sum(wordcount)'])
	return final

def countPosts(posts, sc, ss):
	grouped = posts.groupBy(posts['subreddit'])
	counts = grouped.agg({"*": "count", "wordcount": "sum"})
	return counts


if __name__ == "__main__":
	main()

