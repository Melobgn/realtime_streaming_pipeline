"""
Spark Structured Streaming – Kafka to Delta Silver

This script consumes JSON messages from a Kafka topic (`sensor_data`),
cleans and validates the data, and writes the result to a Delta Lake
Silver table using Spark Structured Streaming.

Kafka offsets and Spark checkpoints ensure fault tolerance.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType
from pyspark.sql.functions import (
    col,
    current_timestamp,
    to_timestamp,
    from_json,
    when,
)

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "sensor_data"

SILVER_PATH = "lake/silver/sensor_data"
CHECKPOINT_PATH = "checkpoints/silver_sensor_data"

DELTA_PACKAGE = "io.delta:delta-spark_2.13:4.0.0"
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"


SENSOR_SCHEMA = (
    StructType()
    .add("timestamp", StringType())
    .add("device_id", StringType())
    .add("building", StringType())
    .add("floor", IntegerType())
    .add("type", StringType())
    .add("value", DoubleType())
    .add("unit", StringType())
)


def build_spark() -> SparkSession:
    """
    Build a SparkSession with Kafka and Delta Lake support.
    """
    return (
        SparkSession.builder
        .appName("Kafka-to-Delta-Silver")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.jars.packages", f"{DELTA_PACKAGE},{KAFKA_PACKAGE}")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def transform(df_kafka: DataFrame) -> DataFrame:
    """
    Parse Kafka messages, clean data, and apply basic validation rules.
    """
    df = (
        df_kafka
        .select(
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("raw_json"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn("parsed", from_json(col("raw_json"), SENSOR_SCHEMA))
        .select(
            "kafka_key",
            "raw_json",
            "topic",
            "partition",
            "offset",
            "kafka_timestamp",
            col("parsed.*"),
        )
    )

    df = (
        df
        .withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ssX"))
        .withColumn("ingest_time", current_timestamp())
        .drop("timestamp")
        .filter(col("device_id").isNotNull())
        .filter(col("building").isNotNull())
        .filter(col("type").isNotNull())
        .filter(col("value").isNotNull())
        .filter(col("event_time").isNotNull())
    )

    # Basic sanity checks
    df = df.filter(
        when(col("type") == "temperature", (col("value") >= -40) & (col("value") <= 120))
        .when(col("type") == "humidity", (col("value") >= 0) & (col("value") <= 100))
        .when(col("type") == "co2", (col("value") >= 0) & (col("value") <= 10000))
        .otherwise(True)
    )

    return df.select(
        "device_id",
        "building",
        "floor",
        "type",
        "value",
        "unit",
        "event_time",
        "ingest_time",
        "topic",
        "partition",
        "offset",
        "kafka_timestamp",
        "kafka_key",
        "raw_json",
    )


def main() -> None:
    """
    Start the Spark Structured Streaming job.
    """
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    df_kafka = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    df_silver = transform(df_kafka)

    query = (
        df_silver.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start(SILVER_PATH)
    )

    print(f"Streaming started. Writing Delta Silver to '{SILVER_PATH}'.")
    query.awaitTermination()


if __name__ == "__main__":
    main()
