"""
Streaming JSON Files to Delta Lake (Bronze)

This script uses Spark Structured Streaming to ingest IoT sensor data from JSON
files, apply basic cleaning and validation, and write the results to a Delta Lake
Bronze table with checkpointing for fault tolerance.
"""

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, current_timestamp, to_timestamp, when

# ----------------------------
# Paths (local project layout)
# ----------------------------
INPUT_DIR = "input/sensor_data"                 
BRONZE_PATH = "lake/bronze/sensor_data"        
CHECKPOINT_PATH = "checkpoints/bronze_sensor"   

# Spark 4.0.1 uses Scala 2.13 => Delta package must be _2.13
DELTA_PACKAGE = "io.delta:delta-spark_2.13:4.0.0"


# ----------------------------
# Expected schema for your JSON lines
# ----------------------------
schema = (
    StructType()
    .add("timestamp", StringType())   # ISO string, parse explicitly
    .add("device_id", StringType())
    .add("building", StringType())
    .add("floor", IntegerType())
    .add("type", StringType())        # "co2", "temperature", "humidity", etc.
    .add("value", DoubleType())       # accept int/float
    .add("unit", StringType())
)


def build_spark(app_name: str) -> SparkSession:
    """
    Builds a SparkSession with Delta Lake configuration enabled.
    Delta jars are resolved via Maven (spark.jars.packages) at session creation time.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.jars.packages", DELTA_PACKAGE)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def transform_bronze(df: DataFrame) -> DataFrame:
    """
    Bronze transformations: parse timestamps, cleanup, basic validation rules, projection.
    """
    # Parse event time (handles "Z" and offsets thanks to X)
    df2 = (
        df
        .withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ssX"))
        .withColumn("ingest_time", current_timestamp())
        .drop("timestamp")
        # Basic required fields
        .filter(col("device_id").isNotNull())
        .filter(col("building").isNotNull())
        .filter(col("type").isNotNull())
        .filter(col("event_time").isNotNull())
        .filter(col("value").isNotNull())
    )

    # Basic sanity checks by type (examples; adjust to your domain)
    df2 = df2.filter(
        when(col("type") == "temperature", (col("value") >= -40) & (col("value") <= 120))
        .when(col("type") == "humidity", (col("value") >= 0) & (col("value") <= 100))
        .when(col("type") == "co2", (col("value") >= 0) & (col("value") <= 10000))
        .otherwise(True)
    )

    # Projection for Bronze table
    return df2.select(
        "device_id",
        "building",
        "floor",
        "type",
        "value",
        "unit",
        "event_time",
        "ingest_time",
    )


def run_streaming() -> None:
    """
    Continuous pipeline:
    JSON files (stream) -> Bronze Delta table with checkpointing.
    """
    spark = build_spark("SmartTech-2.1-Files-JSON-to-Delta-Bronze")
    spark.sparkContext.setLogLevel("WARN")
    print(f"Spark version: {spark.version}")
    print(f"INPUT_DIR={INPUT_DIR}")
    print(f"BRONZE_PATH={BRONZE_PATH}")
    print(f"CHECKPOINT_PATH={CHECKPOINT_PATH}")

    # Streaming source: new JSON files as they appear
    df_raw = (
        spark.readStream
        .schema(schema)   # recommended for file streaming
        .json(INPUT_DIR)
    )

    df_bronze = transform_bronze(df_raw)

    query = (
        df_bronze.writeStream
        .format("delta")
        .outputMode("append")  # Bronze = append
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start(BRONZE_PATH)
    )

    query.awaitTermination()


def run_self_test() -> None:
    """
    Batch self-test: validates parsing and filtering logic without running an infinite streaming query.
    """
    spark = build_spark("SmartTech-2.1-SelfTest")
    spark.sparkContext.setLogLevel("WARN")
    print(f"Spark version: {spark.version}")

    # Data approximating your JSON shape
    data = [
        ("2025-01-12T08:18:00Z", "sensor-co2-020", "B", 1, "co2", 862.0, "ppm"),          # OK
        ("2025-01-12T08:18:04Z", "sensor-temp-003", "B", 3, "temperature", 27.5, "°C"),   # OK
        ("2025-01-12T08:18:07Z", None, "A", 2, "temperature", 26.5, "°C"),                # device_id null -> filtered
        ("2025-01-12T08:18:12Z", "sensor-hum-002", "B", 1, "humidity", 130.3, "%"),       # humidity out of range -> filtered
        ("BAD_TS", "sensor-co2-020", "A", 2, "co2", 1008.0, "ppm"),                        # timestamp invalid -> filtered
    ]

    df = spark.createDataFrame(
        data,
        ["timestamp", "device_id", "building", "floor", "type", "value", "unit"]
    )

    out = transform_bronze(df)

    remaining = [(r["device_id"], r["type"]) for r in out.select("device_id", "type").orderBy("device_id", "type").collect()]
    expected = [("sensor-co2-020", "co2"), ("sensor-temp-003", "temperature")]

    assert remaining == expected, f"Self-test failed: expected {expected}, got {remaining}"
    assert "event_time" in out.columns, "Self-test failed: event_time missing"
    assert "ingest_time" in out.columns, "Self-test failed: ingest_time missing"

    print("Self-test OK: parsing + filtering behave as expected.")


if __name__ == "__main__":
    # Self-test:
    #   RUN_SELF_TEST=1 python streaming_files_to_delta_bronze.py
    #
    # Continuous streaming:
    #   python streaming_files_to_delta_bronze.py
    #
    # Notes:
    # - For file streaming, Spark will process files it detects as new (often after job start).
    # - If you want to reprocess the same files, delete the checkpoint folder (CHECKPOINT_PATH).

    if os.getenv("RUN_SELF_TEST") == "1":
        run_self_test()
    else:
        run_streaming()
