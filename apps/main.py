from pyspark.sql import SparkSession
from pyspark.sql.functions import *


# Init spark session
spark = SparkSession \
  .builder \
  .appName('spark-streaming-twitter') \
  .getOrCreate()

# Read message from kafka consumer
df = spark \
  .read \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:9092") \
  .option("subscribe", "test-topic") \
  .load()


# Convert binary to string
df_to_string = df.withColumn('value_string', df.value.cast('string'))
df_to_string.printSchema()
df_to_string.selectExpr("*").show()
