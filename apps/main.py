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
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "test-topic") \
  .load()

df.printSchema()
df.show()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Convert binary to string
df_to_string = df.withColumn('value_string', df.value.cast('string'))