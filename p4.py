from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType

# Define schema matching Kafka JSON data
schema = StructType() \
    .add("month", StringType()) \
    .add("year", StringType()) \
    .add("products", StringType()) \
    .add("quantity_000_metric_tonnes_", StringType()) \
    .add("updated_date", StringType())

# SparkSession with Kafka + AWS support
spark = SparkSession.builder \
    .appName("KafkaToS3_JSON") \
    .master("local[*]") \
    .config("spark.jars.packages", ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "org.apache.hadoop:hadoop-common:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.11.1026"
])) \
    .getOrCreate()

# Set Hadoop S3 credentials
hadoop_conf = spark._jsc.hadoopConfiguration()
#addaccesskey
#addsecretkey
hadoop_conf.set("fs.s3a.endpoint", "s3.amazonaws.com")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.path.style.access", "true")

# Read from Kafka topic
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "gov-data") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON from Kafka messages
json_df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Write to S3 in JSON format
query = json_df.writeStream \
    .format("json") \
    .option("path", "s3a://govdata-output/data/") \
    .option("checkpointLocation", "/tmp/kafka_s3_json_checkpoint/") \
    .outputMode("append") \
    .start()

query.awaitTermination(10)