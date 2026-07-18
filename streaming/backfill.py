from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime
from delta import configure_spark_with_delta_pip

# 1. Initialize Spark (same as your main.py)
builder = (
    SparkSession.builder.appName("delta-check")
    .master("local[1]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

DELTA_PATH = "/data/delta/solar_data"

# 2. Read Delta, order by collection_time descending, limit to 5
df = (
    spark.read.format("delta")
    .load(DELTA_PATH)
    .orderBy(col("collection_time").desc())
    .limit(5)
)

# 3. Select only the most important columns for readability
important_cols = [
    "device_sn",
    "device_type",
    "source",
    "collection_time",
    "total_active_ac_output_power",
    "daily_active_production",
    "total_consumption_power",
]

print("\n=== LAST 5 ROWS IN DELTA LAKE ===\n")
df.select(
    "device_sn",
    "device_type",
    "source",
    from_unixtime("collection_time").alias("readable_time"),
    "collection_time",
    "total_active_ac_output_power",
    "daily_active_production",
    "total_consumption_power",
).show(truncate=False, vertical=True)  # vertical=True makes it easy to read wide rows

spark.stop()
