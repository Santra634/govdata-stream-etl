from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType

# Define schema based on API's data
schema = StructType() \
    .add("month", StringType()) \
    .add("year", StringType()) \
    .add("products", StringType()) \
    .add("quantity_000_metric_tonnes_", StringType()) \
    .add("updated_date", StringType())


spark = SparkSession.builder \
    .appName("KafkaStreamApp") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# Read stream from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "gov-data") \
    .option("startingOffsets", "earliest") \
    .load()

# Convert Kafka value (bytes) to string and parse JSON
json_df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Print to console
query = json_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination(10)
