import sys
import json
import datetime
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.storagelevel import StorageLevel
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream
from pyspark.sql import Row
from pyspark.sql.types import StructType, IntegerType, DateType
from pyspark.sql.types import *
from pyspark.sql.functions import *


s3OutputPath = ""

# Get existing SparkSession
def getSparkSessionInstance(sparkConf):
	if ("sparkSessionSingletonInstance" not in globals()):
		globals()["sparkSessionSingletonInstance"] = SparkSession.builder.config(conf=sparkConf).getOrCreate()
	return globals()["sparkSessionSingletonInstance"]

# Process each RDD and write to S3
def processRecords(rdd):
	if not rdd.isEmpty():
		spark = getSparkSessionInstance(rdd.context.getConf())
		df = spark.read.json(rdd)
		df.show()
		now = datetime.datetime.now()
		year = now.strftime("%Y")
		month = now.strftime("%m")
		day = now.strftime("%d")
		hour = now.strftime("%H")
		TargetPath = "s3://"+s3OutputPath+"/ingest_year="+ year + "/ingest_month=" + month + "/ingest_day=" + day + "/ingest_hour=" + hour + "/"
		df.write.mode('append').parquet(TargetPath)
			
if __name__ == "__main__":
	if len(sys.argv) != 5:
		print("Usage: kinesis-stream-consumer.py <region-name> <stream-name> <endpoint-url> <s3output-path>", file=sys.stderr)
		sys.exit(-1)

	# Read Runtime arguments
	scriptName, regionName, streamName, endpointUrl, s3OutputPath = sys.argv

	# Initialize SparkSession and StreamingContext
	appName = "ClickStreamEventConsumer"
	sparkSession =  SparkSession.builder.appName(appName).getOrCreate()
	sparkContext = sparkSession.sparkContext
	streamingContext = StreamingContext(sparkContext, 1)
	
	# Start reading from Kinesis Stream
	records = KinesisUtils.createStream(streamingContext, appName, streamName, endpointUrl, regionName, InitialPositionInStream.LATEST, 5, StorageLevel.MEMORY_AND_DISK_2)

	# Process each batch
	records.foreachRDD(lambda rdd: processRecords(rdd))

	# Start StreamingContext
	streamingContext.start()
	streamingContext.awaitTermination()
