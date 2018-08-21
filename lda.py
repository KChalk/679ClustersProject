from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import size, split
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, ArrayType, MapType, StringType


# mlib in spark API docs
def main():
	spark = SparkSession \
		.builder \
		.appName("Reddit:get liwc") \
		.config("spark.some.config.option", "some-value") \
		.getOrCreate()

	size = "large"  # medium or large
	if size == "large":
		file = "l_filtered_posts.csv"
		output="l_output.csv"
	elif size == "medium":
		file = "m_filtered_posts.csv"
		#file = "file:///g/chalkley/Winter18/679Clusters/Project/m_filtered_posts.csv"
		output="m_output.csv"

	else:
		file = "file:///g/chalkley/Winter18/679Clusters/Project/s_filtered_posts.csv"
		output="s_output.csv"

#		file = "file:///g/chalkley/Winter18/679Clusters/Project/redditexcerpt.txt"

	sc = spark.sparkContext
