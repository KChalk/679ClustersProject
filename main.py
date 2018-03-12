from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import size, split
from operator import add
import json


def main():
    spark = SparkSession \
        .builder \
        .appName("Reddit:ETA<20min") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    # Setup Spark with varying data size

    size = "medium"  # medium or large
    if size == "large":
#		conf= SparkConf().setAppName("Reddit:ETA-Substantial")
        file = "RS_full_corpus.bz2"
    elif size == "medium":
#		conf= SparkConf().setAppName("Reddit:ETA<10min")
        file = "RS_2017_11.bz2"
    else:
#		conf= SparkConf().setAppName("Reddit:ETA<5min")
        file = "file:///g/chalkley/Winter18/679Clusters/Project/redditexcerpt.txt"

    sc = spark.sparkContext

    # filter
    postRDD = filterPosts(file, sc, spark)
    # broadcast word lists
    absolutist = ['absolutely', 'all', 'always', 'complete', 'competely''constant', 'constantly', 'definitely',
                  'entire', 'ever', 'every', 'everyone', 'everything', 'full', 'must', 'never', 'nothing', 'totally',
                  'whole']

    # abschars=['a','c','d','e','f','m','n','t','w']

    # freq
    abscounts = calculatePosts(postRDD, absolutist, sc, spark)

    # count
    # subRs=countPosts(postRDD,sc,spark)

    # collect
    collected = abscounts.collect()
    # print
    print('\n\n\n')
    print(collected)


def filterPosts(filename, sc, ss):
    allposts = ss.read.json(filename)

    subreddits = ['depression', 'Anxiety', 'SuicideWatch', 'men', 'women', 'TwoXChromosomes', 'Asthma', 'diabetes',
                  'cancer']

    subRposts = allposts.filter(allposts.subreddit.isin(subreddits) & allposts['is_self'] == True)
    splitified = subRposts.withColumn("stextsplit", split(subRposts['selftext'], ''))
    counted = splitified.withColumn("wordcount", size(splitified['stextsplit']))
    longposts = counted.filter(counted.wordcount >= 100)

    return longposts


def getabs(words, wordlist):
    abs = []
    for word in words:
        if word in wordlist:
            abs.append(word)
    return abs


def calculatePosts(posts, wordlist, sc, ss):
    abstext = posts.withColumn("abswords", getabs(posts['stextsplit'], wordlist))
#    abstext = posts.map(lambda x: x.stextsplit"abswords", getabs(posts['stextsplit'], wordlist))

    abscount = abstext.withColumn("abscount", size(abstext['abswords']))
    #	absfreq=abscount.withColumn("absfreq",abscount['abscount']/abscount['wordcount']

    grouped = abscount.groupBy(abscount.subreddit)
    counts = grouped.agg({"*": "count", "wordcount": "sum", "abscount": "sum"})
    final = counts.withColumn("absfreq", counts['abscount'] / counts['wordcount'])
    return final


def countPosts(posts, sc, ss):
    grouped = posts.groupBy(posts.subreddit)
    counts = grouped.agg({"*": "count", "wordcount": "sum"})
    return counts


if __name__ == "__main__":
    main()

"""
subreddit=u'women', 
sum(wordcount)=16357, 
count(1)=66

subreddit=u'depression', 
sum(wordcount)=1202562, 
count(1)=6383

subreddit=u'SuicideWatch', 
sum(wordcount)=514025, 
count(1)=2716


subreddit=u'Anxiety', 
sum(wordcount)=583720, 
count(1)=3116
"""
