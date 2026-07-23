import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime
from delta import configure_spark_with_delta_pip

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

DELTA_PATHS = {
    "h1": "/data/delta/solar_data",
    "h2": "/data/delta/solar_data_h2",
    "h3": "/data/delta/solar_data_h3",
    "h4": "/data/delta/solar_data_h4",
}

target = sys.argv[1] if len(sys.argv) > 1 else "h1"
delta_path = DELTA_PATHS.get(target)
if not delta_path:
    print(f"Unknown target: {target}. Choose from: {', '.join(DELTA_PATHS.keys())}")
    sys.exit(1)

try:
    df = (
        spark.read.format("delta")
        .load(delta_path)
        .orderBy(col("collection_time").desc())
        .limit(5)
    )
except Exception as e:
    print(f"Error reading {target} delta at {delta_path}: {e}")
    spark.stop()
    sys.exit(0)

print(f"\n=== LAST 5 ROWS IN DELTA LAKE ({target}) ===\n")
df.select(
    "device_sn",
    "device_type",
    "source",
    from_unixtime("collection_time").alias("readable_time"),
    "collection_time",
    "total_active_ac_output_power",
    "daily_active_production",
    "total_consumption_power",
).show(truncate=False, vertical=True)

spark.stop()
