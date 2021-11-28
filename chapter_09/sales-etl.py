import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

if __name__ == "__main__":
	if len(sys.argv) != 3:
		print("Required Arguments Missing")
		exit(-1)

	spark = SparkSession.builder.appName('Sales ETL Job').getOrCreate()

	df = spark.read.format("csv").option("header", "true").load(sys.argv[1])
	replacements = {c:c.replace(' ','_') for c in df.columns if ' ' in c}
	df1 = df.select([col(c).alias(replacements.get(c, c)) for c in df.columns])
	df1.write.parquet(sys.argv[2])
	
	spark.stop()
