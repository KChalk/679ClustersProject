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

 
def main():
	spark = SparkSession \
		.builder \
		.appName("Reddit:get liwc") \
		.config("spark.some.config.option", "some-value") \
		.getOrCreate()

	size = "medium"  # medium or large
	if size == "large":
		file = "file:///g/chalkley/Winter18/679Clusters/Project/l_filtered_posts.csv"
		output="l_output.csv"
	elif size == "medium":
		file = "file:///g/chalkley/Winter18/679Clusters/Project/m_filtered_posts.csv"
		output="m_output.csv"

	else:
		file = "file:///g/chalkley/Winter18/679Clusters/Project/s_filtered_posts.csv"
		output="s_output.csv"

#		file = "file:///g/chalkley/Winter18/679Clusters/Project/redditexcerpt.txt"

	sc = spark.sparkContext


#dicts
	global DICTIONARIES
	print('\n\n\n Starting dict read')
	DICTIONARIES=getdicts("LIWC2007_updated.dic")
	## Save DICTIONARIES?
	## Use as actual broadcast variable

# freq
	postRDD = spark.read.csv(file)
	print('\n\n\n starting freq counts')
	abscounts = calculatePosts2(postRDD, sc, spark)

	if False:
		print('\n\n\n')
		abscounts.printSchema()
		print('abscunts num parttions',abscounts.rdd.getNumPartitions())
		print(abscounts.count())
		print('\n\n\n')

# write
	abscounts.coalesce(100).write.csv(output, header=True)

#	collected=abscounts.collect()
	
#	print('collected')	

#	with open('bigoutput.csv','wb') as outfile:
#		writer = csv.writer(outfile)
#		writer.writerow(abscounts.columns())
#		for row in collected:
#			this_row = [row[colname].encode("utf-8") for colname in abscounts.columns()]
#			writer.writerow(this_row)
					
	
def getdicts(filename):
	boundarycount=0
	dictdict={}
	dictdict['dictnames']={}
	absolutist = set(['absolutely', 'all', 'always', 'complete', 'competely','constant', 'constantly', 'definitely', 'entire', 'ever', 'every', 'everyone', 'everything', 'full', 'must', 'never', 'nothing', 'totally','whole'])

	with open(filename) as file:
		for line in file:
			if line[0] =='%':
				boundarycount+=1
			else:			
				if boundarycount==1: #in list of dicionary names and codes
					line=line.split('\t')
					dictdict['dictnames'][line[0]]=line[1][:-1]
				if boundarycount==2: #in list of words followed by list of dicts they belong to 
					line=line.split('\t')
					word=line[0]
					if word[-1]=='*': #remove *'s-- program will not distinguish between words and prefixes
						word=word[:-1]
					final=line[-1] 
					if final[-1]=='\n': 
						line[-1]=final[:-1]
					else:
						print('file formats are impossible')
					dictdict[word]=set(line[1:])
					if word in absolutist:
						dictdict[word].add('0') #add custom dictionary code
				if boundarycount>2: 
					print('\n\n\n')
					print('error in reading dicts')
					print('\n\n\n')
		dictdict['dictnames']['0']='absolutist' #add custom dictionary name
	print('completed get dicts')
	return dictdict


def getfreqs(text): 
	counts={'0':0,'1':0, '2':0, '3':0,'4':0,'5':0,'6':0,'7':0,'8':0,'9':0,'10':0,'11':0,'12':0,'13':0,'14':0,'15':0,'16':0,'17':0,'18':0,'19':0,'20':0,'21':0,'22':0,'121':0,'122':0,'123':0,'124':0,'125':0,'126':0,'127':0,'128':0,'129':0,'130':0,'131':0,'132':0,'133':0,'134':0,'135':0,'136':0,'137':0,'138':0,'139':0,'140':0,'141':0,'142':0,'143':0,'146':0,'147':0,'148':0,'149':0,'150':0,'250':0,'251':0,'252':0,'253':0,'354':0,'355':0,'356':0,'357':0,'358':0,'359':0,'360':0,'462':0,'463':0,'464':0}
	words=text.split()

	for word in words:
		word=word.lower()
		i=len(word)
		while i>0:
			try:
				for dict in DICTIONARIES[word[:i]]:
					counts[dict]+=1
				i=0 
			except KeyError: 
				i-=1
		
	print('completed get freqs')
	return counts

def calculatePosts2(posts, sc, ss):
	getfreqsUDF = udf(getfreqs, MapType(StringType(), IntegerType()))
	
	#count words per dictionary
	countdict = posts.withColumn("counts", getfreqsUDF('selftext'))
	
	#initialize new df without self text
	tidyDF=countdict.select('id','subreddit','wordcount','counts')
	
	#move counts from 'counts' column of dicts to wide columns
	
	for dict in DICTIONARIES['dictnames']:
		tidyDF=tidyDF.withColumn(DICTIONARIES['dictnames'][dict], tidyDF['counts'][dict]) 
		print('added',dict) 
	
	#drop 'counts' column
	tidyDF=tidyDF.drop('counts')
	
	#aggregate per dict counts by subreddit
	grouped = tidyDF.groupBy(tidyDF['subreddit'])
	counts = grouped.agg({"*": "count", "wordcount": "sum", 'absolutist': "sum",'funct' : "sum", 'pronoun' : "sum", 'ppron' : "sum", 'i' : "sum", 'we' : "sum", 'you' : "sum", 'shehe' : "sum", 'they' : "sum", 'ipron' : "sum", 'article' : "sum", 'verb' : "sum", 'auxverb' : "sum", 'past' : "sum", 'present' : "sum", 'future' : "sum", 'adverb' : "sum", 'preps' : "sum", 'conj' : "sum", 'negate' : "sum", 'quant' : "sum", 'number' : "sum", 'swear' : "sum", 'social' : "sum", 'family' : "sum", 'friend' : "sum", 'humans' : "sum", 'affect' : "sum", 'posemo' : "sum", 'negemo' : "sum", 'anx' : "sum", 'anger' : "sum", 'sad' : "sum", 'cogmech' : "sum", 'insight' : "sum", 'cause' : "sum", 'discrep' : "sum", 'tentat' : "sum", 'certain' : "sum", 'inhib' : "sum", 'incl' : "sum", 'excl' : "sum", 'percept' : "sum", 'see' : "sum", 'hear' : "sum", 'feel' : "sum", 'bio' : "sum", 'body' : "sum", 'health' : "sum", 'sexual' : "sum", 'ingest' : "sum", 'relativ' : "sum", 'motion' : "sum", 'space' : "sum", 'time' : "sum", 'work' : "sum", 'achieve' : "sum", 'leisure' : "sum", 'home' : "sum", 'money' : "sum", 'relig' : "sum", 'death' : "sum", 'assent' : "sum", 'nonfl' : "sum", 'filler' : "sum"})
	
	counts = counts.filter(counts['count(1)']>=100)

	print('finished group with filter' )

	#initialize new df
	tidyfreqDF=counts

	#convert counts to frequencies
	for dict in DICTIONARIES['dictnames']:
		dictname=DICTIONARIES['dictnames'][dict]

		tidyfreqDF=tidyfreqDF.withColumn(dictname+'freq', tidyfreqDF['sum('+dictname+')']/tidyfreqDF['sum(wordcount)'])

		tidyfreqDF=tidyfreqDF.drop(tidyfreqDF['sum('+dictname+')'])
		
		print('added part 2',dict)

	#return DF of 111k rows (subreddits) by 60 columns (per dict freqs)
	return tidyfreqDF

if __name__ == "__main__":
	main()
 
