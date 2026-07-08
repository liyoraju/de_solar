from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    LongType,
)
from delta import configure_spark_with_delta_pip
import os

builder = (
    SparkSession.builder.appName("solar-pipeline")
    .master("local[2]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.shuffle.partitions", "4")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

spark.sparkContext.setLogLevel("WARN")  # <- reduce log spam
print("- Spark + Delta started")

# Schema matching your InverterData Pydantic model
inverter_schema = StructType(
    [
        StructField("device_sn", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("device_state", IntegerType(), True),
        StructField("collection_time", LongType(), True),
        # Rated Power
        StructField("rated_power", DoubleType(), True),
        # DC Voltages
        StructField("dc_voltage_pv1", DoubleType(), True),
        StructField("dc_voltage_pv2", DoubleType(), True),
        StructField("dc_voltage_pv3", DoubleType(), True),
        StructField("dc_voltage_pv4", DoubleType(), True),
        StructField("dc_voltage_pv5", DoubleType(), True),
        StructField("dc_voltage_pv6", DoubleType(), True),
        StructField("dc_voltage_pv7", DoubleType(), True),
        StructField("dc_voltage_pv8", DoubleType(), True),
        # DC Currents
        StructField("dc_current_pv1", DoubleType(), True),
        StructField("dc_current_pv2", DoubleType(), True),
        StructField("dc_current_pv3", DoubleType(), True),
        StructField("dc_current_pv4", DoubleType(), True),
        StructField("dc_current_pv5", DoubleType(), True),
        StructField("dc_current_pv6", DoubleType(), True),
        StructField("dc_current_pv7", DoubleType(), True),
        StructField("dc_current_pv8", DoubleType(), True),
        # DC Power
        StructField("dc_power_pv1", DoubleType(), True),
        StructField("dc_power_pv2", DoubleType(), True),
        StructField("dc_power_pv3", DoubleType(), True),
        StructField("dc_power_pv4", DoubleType(), True),
        StructField("dc_power_pv5", DoubleType(), True),
        StructField("dc_power_pv6", DoubleType(), True),
        StructField("dc_power_pv7", DoubleType(), True),
        StructField("dc_power_pv8", DoubleType(), True),
        # AC Voltage
        StructField("ac_voltage_rua", DoubleType(), True),
        StructField("ac_voltage_svb", DoubleType(), True),
        StructField("ac_voltage_twc", DoubleType(), True),
        # AC Current
        StructField("ac_current_rua", DoubleType(), True),
        StructField("ac_current_svb", DoubleType(), True),
        StructField("ac_current_twc", DoubleType(), True),
        # Frequency / Power
        StructField("ac_output_frequency_r", DoubleType(), True),
        StructField("total_active_ac_output_power", DoubleType(), True),
        # Line Voltages
        StructField("ab_line_voltage", DoubleType(), True),
        StructField("bc_line_voltage", DoubleType(), True),
        StructField("ac_line_voltage", DoubleType(), True),
        # Production
        StructField("total_active_production", DoubleType(), True),
        StructField("daily_active_production", DoubleType(), True),
        # Inverter Output Power
        StructField("inverter_output_power_l1", DoubleType(), True),
        StructField("inverter_output_power_l2", DoubleType(), True),
        StructField("inverter_output_power_l3", DoubleType(), True),
        # Energy
        StructField("total_grid_feed_in", DoubleType(), True),
        StructField("total_energy_purchased", DoubleType(), True),
        StructField("total_consumption_power", DoubleType(), True),
        StructField("total_consumption", DoubleType(), True),
    ]
)

# Read from Kafka
raw_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "broker:29092")
    .option("subscribe", "raw_data")
    .option("startingOffsets", "latest")
    .load()
)

# Parse JSON from Kafka value
parsed_df = raw_df.select(
    from_json(col("value").cast("string"), inverter_schema).alias("data")
).select("data.*")

# Write to Delta Lake
query = (
    parsed_df.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/data/delta/raw/_checkpoints")
    .trigger(processingTime="60 seconds")
    .start("/data/delta/raw")
)

TIMESCALE_USER = os.getenv("TIMESCALE_USER")
TIMESCALE_PASSWORD = os.getenv("TIMESCALE_PASSWORD")
TIMESCALE_DB = os.getenv("TIMESCALE_DB")


# Define the connection properties for TimescaleDB (Postgres)
jdbc_url = f"jdbc:postgresql://timescale_db:5432/{TIMESCALE_DB}"
connection_properties = {
    "user": TIMESCALE_USER,
    "password": TIMESCALE_PASSWORD,
    "driver": "org.postgresql.Driver",
}


# The function that will be executed for every micro-batch
def write_to_timescale(batch_df, batch_id):
    # Derive the hypertable time column from collection_time (epoch seconds)
    ts_df = batch_df.withColumn(
        "time", from_unixtime(col("collection_time")).cast("timestamp")
    )

    # Write the micro-batch to TimescaleDB
    ts_df.write.jdbc(
        url=jdbc_url,
        table="raw_solar",  # The name of your Timescale hypertable
        mode="append",  # Always use append for streaming time-series
        properties=connection_properties,
    )


# Start the stream and apply the foreachBatch function
query = (
    parsed_df.writeStream.foreachBatch(write_to_timescale)
    .outputMode("append")
    .option("checkpointLocation", "/data/tsdb/raw/_checkpoints")
    .start()
)


print("- Streaming query started (Delta Lake:/data/delta/raw)")
spark.streams.awaitAnyTermination()
