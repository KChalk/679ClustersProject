from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import size, split
from operator import add
import json
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, ArrayType

'''
author=u'savagewit', 
is_self=False, 
name=u't3_5yarj', 
permalink=u'/r/reddit.com/comments/5yarj/playing_with_knives_is_risky_but_strange_women/', 
selftext=u'', 
subreddit=u'reddit.com', 
 given input columns: 
 [subreddit, 
 retrieved_on, 
 hidden, 
 domain, 
 selftext, 
 third_party_trackers, 
 is_video, 
 id, secure_media_embed, locked, adserver_imp_pixel, is_self, permalink, promoted_display_name, author, archived, stickied, whitelist_status, href_url, mobile_ad_url, edited, is_reddit_media_domain, suggested_sort, spoiler, parent_whitelist_status, media, thumbnail_height, adserver_click_url, thumbnail_width, promoted_by, promoted, embed_type, hide_score, third_party_tracking, subreddit_type, score, media_embed, secure_media, author_flair_css_class, thumbnail, author_id, crosspost_parent_list, gilded, author_cakeday, link_flair_text, subreddit_id, link_flair_css_class, title, is_crosspostable, promoted_url, original_link, brand_safe, num_comments, created_utc, crosspost_parent, url, post_hint, pinned, author_flair_text, over_18, num_crossposts, disable_comments, domain_override, contest_mode, distinguished, third_party_tracking_2, embed_url, preview];;
'''

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

    # abschars=['a','c','d','e','f','m','n','t','w']

    # freq
    abscounts = calculatePosts(postRDD, sc, spark)

    # count
    # subRs=countPosts(postRDD,sc,spark)

    # collect
#    collected = postRDD.collect()

    collected = abscounts.collect()
    # print
    print('\n\n\n')
    print(collected)


def splitlen(s):
    tokens=s.split()
    return len(tokens)

def mysplit(s):
    tokens=s.split()
    return tokens

def filterPosts(filename, sc, ss):
    allposts = ss.read.json(filename)
    subreddits = set(['depression', 'Anxiety', 'SuicideWatch', 'men', 'women', 'TwoXChromosomes', 'Asthma', 'diabetes', 'cancer','reddit.com'])
    subRposts = allposts.filter(allposts['subreddit'].isin(subreddits) & allposts['is_self'] == True)
    splitlenUDF = udf(splitlen, IntegerType())
    bettercols=subRposts.select('id','subreddit','selftext',splitlenUDF('selftext').alias("wordcount"))
    longposts = bettercols.filter(bettercols['wordcount'] >= 100)
    return longposts


def getabs(text):
    abscount =0
    words=text.split()
    absolutist = set(['absolutely', 'all', 'always', 'complete', 'competely''constant', 'constantly', 'definitely', 'entire', 'ever', 'every', 'everyone', 'everything', 'full', 'must', 'never', 'nothing', 'totally','whole'])
    for word in words:
        if word in absolutist:
            abscount+=1
    return abscount


def calculatePosts(posts, sc, ss):
    getabsUDF = udf(getabs, IntegerType())

    abscount = posts.withColumn("abscount", getabsUDF('selftext'))


#    abscount = abstext.withColumn("abscount", size(abstext['abswords']))
    #	absfreq=abscount.withColumn("absfreq",abscount['abscount']/abscount['wordcount']
    #abscount=posts.withColumn("abscout",getabs(posts

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

