from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, decode, window, first, last, sum, count, avg, min, max, from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
import os

checkpoint_dir = os.getenv("CHECKPOINT_DIR", "./checkpoints")

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC  = "btc-trades"

schema = StructType([
    StructField("symbol",   StringType(), True),
    StructField("price",    StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("side",     StringType(), True),
    StructField("time",     LongType(),   True)
])

spark = (
    SparkSession.builder
    .appName("CryptoKafkaConsumer")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,"
        "org.postgresql:postgresql:42.7.3"
    )
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

agg_df = (
    kafka_df
    .withColumn("value",      decode(col("value"), "UTF-8"))
    .withColumn("data",       from_json(col("value"), schema))
    .withColumn("price",      col("data.price").cast(DoubleType()))
    .withColumn("quantity",   col("data.quantity").cast(DoubleType()))
    .withColumn("symbol",     col("data.symbol"))
    .withColumn("event_time", (col("data.time") / 1000).cast("timestamp"))
    .withWatermark("event_time", "5 seconds")
    .groupBy(window(col("event_time"), "30 seconds"), col("symbol"))
    .agg(
        first("price").alias("open"),
        max("price").alias("high"),
        min("price").alias("low"),
        last("price").alias("close"),
        sum("quantity").alias("volume"),
        count("*").alias("trade_count"),
        avg("price").alias("vwap")
    )
    .select(
        col("window.start").cast("timestamp").alias("time"),
        col("symbol"),
        col("open"),
        col("high"),
        col("low"),
        col("close"),
        col("volume"),
        col("trade_count"),
        col("vwap")
    )
)

def write_to_postgres(batch_df: DataFrame, batch_id: int):
    (batch_df.write
     .format("jdbc")
     .option("url",      "jdbc:postgresql://localhost:5432/crypto")
     .option("dbtable",  "ohlcv_1m")
     .option("user",     "postgres")
     .option("password", "password")
     .option("driver",   "org.postgresql.Driver")
     .mode("append")
     .save())

query = (
    agg_df.writeStream
    .foreachBatch(write_to_postgres)
    .option("checkpointLocation", checkpoint_dir)
    .start()
)

query.awaitTermination()