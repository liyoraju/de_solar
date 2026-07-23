import os
import time
import threading
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    from_unixtime,
    lit,
    when,
    unix_timestamp,
    concat,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    LongType,
)
from pyspark.sql.utils import AnalysisException
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

builder = (
    SparkSession.builder.appName("solar-pipeline")
    .master("local[4]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.scheduler.mode", "FAIR")
    .config("spark.driver.memory", "1536m")
    .config("spark.executor.memory", "1536m")
    .config("spark.memory.fraction", "0.7")
    .config("spark.memory.storageFraction", "0.3")
    .config("spark.sql.shuffle.partitions", "2")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")
logging.info("- Spark + Delta started")

DELTA_PATH_H1 = "/data/delta/solar_data"
DELTA_PATH_H2 = "/data/delta/solar_data_h2"
DELTA_PATH_H3 = "/data/delta/solar_data_h3"
DELTA_PATH_H4 = "/data/delta/solar_data_h4"

TS_TABLE_H1 = "raw_solar"
TS_TABLE_H2 = "raw_solar_h2"
TS_TABLE_H3 = "raw_solar_h3"
TS_TABLE_H4 = "raw_solar_h4"

BOOTSTRAP_SERVERS = "broker:29092"
DELTA_PATH = DELTA_PATH_H1

# ── Validate env vars early ────────────────────────────────────────────────────
TIMESCALE_USER = os.getenv("TIMESCALE_USER")
TIMESCALE_PASSWORD = os.getenv("TIMESCALE_PASSWORD")
TIMESCALE_DB = os.getenv("TIMESCALE_DB")

if not all([TIMESCALE_USER, TIMESCALE_PASSWORD, TIMESCALE_DB]):
    raise EnvironmentError(
        "Missing required env vars: TIMESCALE_USER, TIMESCALE_PASSWORD, TIMESCALE_DB"
    )

jdbc_url = f"jdbc:postgresql://timescale_db:5432/{TIMESCALE_DB}"
jdbc_write_properties = {
    "user": TIMESCALE_USER,
    "password": TIMESCALE_PASSWORD,
    "driver": "org.postgresql.Driver",
    "batchsize": "5000",
    "numPartitions": "2",
}

# ── Schemas ────────────────────────────────────────────────────────────────────
metric_fields = [
    StructField("rated_power", DoubleType(), True),
    StructField("dc_voltage_pv1", DoubleType(), True),
    StructField("dc_voltage_pv2", DoubleType(), True),
    StructField("dc_voltage_pv3", DoubleType(), True),
    StructField("dc_voltage_pv4", DoubleType(), True),
    StructField("dc_voltage_pv5", DoubleType(), True),
    StructField("dc_voltage_pv6", DoubleType(), True),
    StructField("dc_voltage_pv7", DoubleType(), True),
    StructField("dc_voltage_pv8", DoubleType(), True),
    StructField("dc_current_pv1", DoubleType(), True),
    StructField("dc_current_pv2", DoubleType(), True),
    StructField("dc_current_pv3", DoubleType(), True),
    StructField("dc_current_pv4", DoubleType(), True),
    StructField("dc_current_pv5", DoubleType(), True),
    StructField("dc_current_pv6", DoubleType(), True),
    StructField("dc_current_pv7", DoubleType(), True),
    StructField("dc_current_pv8", DoubleType(), True),
    StructField("dc_power_pv1", DoubleType(), True),
    StructField("dc_power_pv2", DoubleType(), True),
    StructField("dc_power_pv3", DoubleType(), True),
    StructField("dc_power_pv4", DoubleType(), True),
    StructField("dc_power_pv5", DoubleType(), True),
    StructField("dc_power_pv6", DoubleType(), True),
    StructField("dc_power_pv7", DoubleType(), True),
    StructField("dc_power_pv8", DoubleType(), True),
    StructField("ac_voltage_rua", DoubleType(), True),
    StructField("ac_voltage_svb", DoubleType(), True),
    StructField("ac_voltage_twc", DoubleType(), True),
    StructField("ac_current_rua", DoubleType(), True),
    StructField("ac_current_svb", DoubleType(), True),
    StructField("ac_current_twc", DoubleType(), True),
    StructField("ac_output_frequency_r", DoubleType(), True),
    StructField("total_active_ac_output_power", DoubleType(), True),
    StructField("ab_line_voltage", DoubleType(), True),
    StructField("bc_line_voltage", DoubleType(), True),
    StructField("ac_line_voltage", DoubleType(), True),
    StructField("total_active_production", DoubleType(), True),
    StructField("daily_active_production", DoubleType(), True),
    StructField("inverter_output_power_l1", DoubleType(), True),
    StructField("inverter_output_power_l2", DoubleType(), True),
    StructField("inverter_output_power_l3", DoubleType(), True),
    StructField("total_grid_feed_in", DoubleType(), True),
    StructField("total_energy_purchased", DoubleType(), True),
    StructField("total_consumption_power", DoubleType(), True),
    StructField("total_consumption", DoubleType(), True),
]

base_fields = [
    StructField("device_sn", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("device_state", IntegerType(), True),
    StructField("collection_time", LongType(), True),
    StructField("granularity", IntegerType(), True),
    StructField("source", StringType(), True),
]

unified_schema = StructType(base_fields + metric_fields)

base_fields_aggregate = [
    StructField("device_sn", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("device_state", IntegerType(), True),
    StructField("collection_time", StringType(), True),  # <-- Changed to String
    StructField("granularity", IntegerType(), True),
    StructField("source", StringType(), True),
]

aggregate_metric_fields = [
    StructField("total_active_production", DoubleType(), True),
    StructField("total_grid_feed_in", DoubleType(), True),
    StructField("total_consumption", DoubleType(), True),
    StructField("total_energy_purchased", DoubleType(), True),
]

aggregate_schema = StructType(base_fields_aggregate + aggregate_metric_fields)

METRIC_COLUMNS = [f.name for f in metric_fields]

TIMESCALE_COLUMNS = [
    "time",
    "device_sn",
    "device_type",
    "device_state",
    "collection_time",
    "granularity",
    "source",
] + METRIC_COLUMNS


# ── Delta table init ───────────────────────────────────────────────────────────
def ensure_delta_table(path, label, schema):
    try:
        spark.sql(f"DESCRIBE EXTENDED delta.`{path}`").collect()
        logging.info(f"- Delta table [{label}] exists at {path}")
    except AnalysisException:
        logging.info(f"- Creating Delta table [{label}] at {path}...")
        spark.createDataFrame([], schema).write.format("delta").save(path)
        logging.info(f"- Delta table [{label}] created")


def wait_for_kafka_topic(topic, retries=16, delay=30):
    for i in range(retries):
        try:
            spark.read.format("kafka").option(
                "kafka.bootstrap.servers", BOOTSTRAP_SERVERS
            ).option("subscribe", topic).option("startingOffsets", "earliest").option(
                "endingOffsets", "latest"
            ).load().count()
            logging.info(f"- Kafka topic '{topic}' ready")
            return
        except Exception as e:
            logging.warning(
                f"- Waiting for Kafka topic '{topic}' ({i + 1}/{retries}): {e}"
            )
            time.sleep(delay)
    logging.warning(f"- WARNING: Kafka topic '{topic}' not confirmed, proceeding")


ensure_delta_table(DELTA_PATH_H1, "h1", unified_schema)
ensure_delta_table(DELTA_PATH_H2, "h2", aggregate_schema)
ensure_delta_table(DELTA_PATH_H3, "h3", aggregate_schema)
ensure_delta_table(DELTA_PATH_H4, "h4", aggregate_schema)
wait_for_kafka_topic("h1_data")
wait_for_kafka_topic("h2_data")
wait_for_kafka_topic("h3_data")
wait_for_kafka_topic("h4_data")
wait_for_kafka_topic("raw_data")

# ── Source schemas ─────────────────────────────────────────────────────────────
h1_schema = StructType(
    [
        StructField("device_sn", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("granularity", IntegerType(), True),
        StructField("collection_time", StringType(), True),
    ]
    + metric_fields
)

realtime_schema = StructType(
    [
        StructField("device_sn", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("device_state", IntegerType(), True),
        StructField("collection_time", LongType(), True),
    ]
    + metric_fields
)


# ── TimescaleDB helpers ────────────────────────────────────────────────────────
def execute_sql(sql):
    """Execute raw SQL against TimescaleDB using the Spark JVM JDBC driver."""
    props = spark._jvm.java.util.Properties()
    props.setProperty("user", TIMESCALE_USER)
    props.setProperty("password", TIMESCALE_PASSWORD)
    driver = spark._jvm.org.postgresql.Driver()
    conn = driver.connect(jdbc_url, props)
    try:
        conn.setAutoCommit(False)
        stmt = conn.createStatement()
        stmt.execute(sql)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


def write_to_timescale(df, label="batch", table_name=TS_TABLE_H1, columns=None):
    staging = f"{table_name}_staging_{uuid.uuid4().hex[:8]}"
    if columns:
        df = df.select(*columns)
    cols = ", ".join(df.columns)

    try:
        df.write.jdbc(
            url=jdbc_url + "?reWriteBatchedInserts=true",
            table=staging,
            mode="overwrite",
            properties=jdbc_write_properties,
        )

        execute_sql(f"""
            INSERT INTO {table_name} ({cols})
            SELECT {cols} FROM {staging}
            ON CONFLICT (device_sn, collection_time, time) DO NOTHING;

            DROP TABLE IF EXISTS {staging};
        """)
        logging.info(f"TimescaleDB write complete [{label}] -> {table_name}")

    except Exception as e:
        try:
            execute_sql(f"DROP TABLE IF EXISTS {staging};")
        except Exception:
            pass
        raise e


def get_last_synced_ts(table_name=TS_TABLE_H1):
    try:
        df = (
            spark.read.format("jdbc")
            .options(
                url=jdbc_url,
                dbtable=f"(SELECT COALESCE(MAX(collection_time), 0) AS max_ts FROM {table_name}) AS t",
                user=TIMESCALE_USER,
                password=TIMESCALE_PASSWORD,
                driver="org.postgresql.Driver",
            )
            .load()
        )
        return df.first()[0]
    except Exception as e:
        logging.warning(
            f"- Could not fetch last synced ts for {table_name}, defaulting to 0: {e}"
        )
        return 0


def sync_delta_to_timescaledb(
    delta_path=DELTA_PATH_H1, timescale_table=TS_TABLE_H1, label="h1", columns=None
):
    logging.info(f"Sync loop [{label}]: checking for unsynced records...")
    last_ts = get_last_synced_ts(timescale_table)
    logging.info(f"Sync loop [{label}]: last synced timestamp = {last_ts}")

    df = (
        spark.read.format("delta")
        .load(delta_path)
        .filter(col("collection_time") > last_ts)
        .withColumn("time", from_unixtime(col("collection_time")).cast("timestamp"))
    )

    if columns:
        df = df.select(*columns)

    if df.limit(1).count() == 0:
        logging.info(f"Sync loop [{label}]: nothing to sync")
        return

    write_to_timescale(
        df, label=f"sync_{label}", table_name=timescale_table, columns=columns
    )
    logging.info(f"Sync loop [{label}]: sync complete")


def sync_loop(
    delta_path=DELTA_PATH_H1, timescale_table=TS_TABLE_H1, label="h1", columns=None
):
    while True:
        time.sleep(300)
        try:
            sync_delta_to_timescaledb(delta_path, timescale_table, label, columns)
        except Exception as e:
            logging.error(f"Sync loop [{label}] error: {e}")


def process_h_batch(topic, label, delta_path, timescale_table):
    df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )

    parsed = df.select(
        from_json(col("value").cast("string"), aggregate_schema).alias("data")
    ).select("data.*")

    count = parsed.count()
    if count > 0:
        logging.info(f"- Processing {count} {label} records...")
        deduped = parsed.dropDuplicates(["device_sn", "collection_time"])

        # Parse collection_time strictly based on granularity string format
        deduped = deduped.withColumn(
            "collection_time",
            when(
                col("granularity") == 2,
                unix_timestamp(col("collection_time"), "yyyy-M-d"),  # e.g., "2026-7-01"
            )
            .when(
                col("granularity") == 3,
                unix_timestamp(
                    concat(col("collection_time"), lit("-01")), "yyyy-M-d"
                ),  # e.g., "2026-7" -> "2026-7-01"
            )
            .when(
                col("granularity") == 4,
                unix_timestamp(
                    concat(col("collection_time"), lit("-01-01")), "yyyy-M-d"
                ),  # e.g., "2026" -> "2026-01-01"
            ),
        )

        # Convert to timestamp
        deduped = deduped.withColumn(
            "time", from_unixtime(col("collection_time")).cast("timestamp")
        )

        delta_table = DeltaTable.forPath(spark, delta_path)
        delta_table.alias("target").merge(
            deduped.alias("source"),
            "target.device_sn = source.device_sn "
            "AND target.collection_time = source.collection_time",
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        logging.info(f"- {label} backfill to Delta complete")

        write_to_timescale(
            deduped, label=f"backfill_{label}", table_name=timescale_table
        )
        logging.info(f"- {label} backfill to TimescaleDB complete")
    else:
        logging.info(f"- No {label} data found")


# ── foreachBatch: dual write (Delta + TimescaleDB) ────────────────────────────
def upsert_to_delta_and_timescale(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    deduped_df = batch_df.dropDuplicates(["device_sn", "collection_time"])

    # Realtime stream is always H1 (granularity == None/1), just convert epoch
    deduped_df = deduped_df.withColumn(
        "time", from_unixtime(col("collection_time")).cast("timestamp")
    )

    deduped_df = deduped_df.cache()

    delta_table = DeltaTable.forPath(spark, DELTA_PATH)
    delta_table.alias("target").merge(
        deduped_df.alias("source"),
        "target.device_sn = source.device_sn "
        "AND target.collection_time = source.collection_time",
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    try:
        write_to_timescale(
            deduped_df, label=f"batch_{batch_id}", columns=TIMESCALE_COLUMNS
        )
    except Exception as e:
        logging.error(
            f"Batch {batch_id}: TimescaleDB write failed — sync_loop will recover: {e}"
        )
    finally:
        deduped_df.unpersist()


# =====================================================================
# PHASE 0: Batch Backfill for h2/h3/h4 (daily/monthly/yearly aggregates)
# =====================================================================
logging.info("- Starting batch backfill from h2_data...")
process_h_batch("h2_data", "h2", DELTA_PATH_H2, TS_TABLE_H2)

logging.info("- Starting batch backfill from h3_data...")
process_h_batch("h3_data", "h3", DELTA_PATH_H3, TS_TABLE_H3)

logging.info("- Starting batch backfill from h4_data...")
process_h_batch("h4_data", "h4", DELTA_PATH_H4, TS_TABLE_H4)

# =====================================================================
# PHASE 1: Batch Backfill (h1_data historical data)
# =====================================================================
logging.info("- Starting batch backfill from h1_data...")

h1_df = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("subscribe", "h1_data")
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
)

parsed_h1_df = (
    h1_df.select(from_json(col("value").cast("string"), h1_schema).alias("data"))
    .select("data.*")
    .withColumn("collection_time", col("collection_time").cast(LongType()))
    .withColumn("device_state", lit(None).cast(IntegerType()))
    .withColumn("source", lit("backfill"))
)

backfill_count = parsed_h1_df.count()
if backfill_count > 0:
    logging.info(f"- Processing {backfill_count} backfill records...")
    deduped_h1_df = parsed_h1_df.dropDuplicates(["device_sn", "collection_time"])

    delta_table = DeltaTable.forPath(spark, DELTA_PATH)
    delta_table.alias("target").merge(
        deduped_h1_df.alias("source"),
        "target.device_sn = source.device_sn "
        "AND target.collection_time = source.collection_time",
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    logging.info("- Backfill to Delta complete")
else:
    logging.info("- No backfill data found in h1_data")

sync_delta_to_timescaledb(DELTA_PATH_H1, TS_TABLE_H1, "h1", columns=TIMESCALE_COLUMNS)

# =====================================================================
# PHASE 2: Realtime Ingestion (Kafka -> Delta -> TimescaleDB)
# =====================================================================
realtime_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("subscribe", "raw_data")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

parsed_realtime_df = (
    realtime_df.select(
        from_json(col("value").cast("string"), realtime_schema).alias("data")
    )
    .select("data.*")
    .withColumn("granularity", lit(None).cast(IntegerType()))
    .withColumn("source", lit("realtime"))
)

realtime_delta_query = (
    parsed_realtime_df.writeStream.foreachBatch(upsert_to_delta_and_timescale)
    .outputMode("update")
    .option("checkpointLocation", f"{DELTA_PATH_H1}/_checkpoints/realtime")
    .trigger(processingTime="30 seconds")
    .start()
)

logging.info("- All streaming queries successfully initiated.")

sync_threads = [
    threading.Thread(
        target=sync_loop,
        args=(DELTA_PATH_H1, TS_TABLE_H1, "h1", TIMESCALE_COLUMNS),
        daemon=True,
    ),
    threading.Thread(
        target=sync_loop, args=(DELTA_PATH_H2, TS_TABLE_H2, "h2"), daemon=True
    ),
    threading.Thread(
        target=sync_loop, args=(DELTA_PATH_H3, TS_TABLE_H3, "h3"), daemon=True
    ),
    threading.Thread(
        target=sync_loop, args=(DELTA_PATH_H4, TS_TABLE_H4, "h4"), daemon=True
    ),
]
for t in sync_threads:
    t.start()

realtime_delta_query.awaitTermination()
