from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf

 
def main():
	spark = SparkSession \
		.builder \
		.appName("TestingZone") \
		.config("spark.some.config.option", "some-value") \
		.getOrCreate()
	
	sc = spark.sparkContext

	dictionary={'a':5, 'b':10,'c,':15}

	bd=sc.broadcast(dictionary)

	innerfunc(bd)

def innerfunc(bd):
	for entry in bd.value:
		print(bd.value[entry])

if __name__ == "__main__":
	main()