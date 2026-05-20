from pyspark.sql import SparkSession

KAFKA_BROKER    = "localhost:9092"
KAFKA_TOPIC     = "btc-trades"


spark = (
    SparkSession.builder
    .appName("CryptoKafkaConsumer")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0"
    )
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

query = (
    df.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", False)
    .start()
)

query.awaitTermination()