from pyspark.sql import SparkSession

BRONZE_PATH = "lake/bronze/sensor_data"

spark = (
    SparkSession.builder
    .appName("Read-Bronze")
    .master("local[*]")
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.0.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

df_bronze_read = spark.read.format("delta").load(BRONZE_PATH)

print("\nLecture des données écrites dans la table Delta (Bronze) :")
df_bronze_read.show(5, truncate=False)
