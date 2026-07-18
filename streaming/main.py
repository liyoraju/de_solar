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

DELTA_PATH = "/data/delta/solar_data"
BOOTSTRAP_SERVERS = "broker:29092"

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
def ensure_delta_table():
    try:
        spark.sql(f"DESCRIBE EXTENDED delta.`{DELTA_PATH}`").collect()
        logging.info(f"- Delta table exists at {DELTA_PATH}")
    except AnalysisException:
        logging.info(f"- Creating Delta table at {DELTA_PATH}...")
        spark.createDataFrame([], unified_schema).write.format("delta").save(DELTA_PATH)
        logging.info("- Delta table created")


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


ensure_delta_table()
wait_for_kafka_topic("h1_data")
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


def write_to_timescale(df, label="batch"):
    """
    Write a DataFrame to TimescaleDB idempotently via a staging table.

    Flow:
      1. Write batch to a temp staging table (no constraints — always succeeds)
      2. INSERT from staging into raw_solar with ON CONFLICT DO NOTHING
      3. Drop staging table

    This makes every write safe to retry — no duplicate key errors regardless
    of how many times foreachBatch or sync_loop runs for the same records.

    Requires on TimescaleDB:
        ALTER TABLE raw_solar
        ADD CONSTRAINT raw_solar_pkey PRIMARY KEY (device_sn, collection_time);
    """
    staging = f"raw_solar_staging_{uuid.uuid4().hex[:8]}"
    cols = ", ".join(TIMESCALE_COLUMNS)

    try:
        # Step 1: write to staging (plain overwrite, no constraint checks)
        df.select(*TIMESCALE_COLUMNS).write.jdbc(
            url=jdbc_url + "?reWriteBatchedInserts=true",
            table=staging,
            mode="overwrite",
            properties=jdbc_write_properties,
        )

        # Step 2: upsert from staging → raw_solar, skip existing rows
        execute_sql(f"""
            INSERT INTO raw_solar ({cols})
            SELECT {cols} FROM {staging}
            ON CONFLICT (device_sn, collection_time, time) DO NOTHING;

            DROP TABLE IF EXISTS {staging};
        """)
        logging.info(f"TimescaleDB write complete [{label}]")

    except Exception as e:
        try:
            execute_sql(f"DROP TABLE IF EXISTS {staging};")
        except Exception:
            pass
        raise e


def get_last_synced_ts():
    try:
        df = (
            spark.read.format("jdbc")
            .options(
                url=jdbc_url,
                dbtable="(SELECT COALESCE(MAX(collection_time), 0) AS max_ts FROM raw_solar) AS t",
                user=TIMESCALE_USER,
                password=TIMESCALE_PASSWORD,
                driver="org.postgresql.Driver",
            )
            .load()
        )
        return df.first()[0]
    except Exception as e:
        logging.warning(f"- Could not fetch last synced ts, defaulting to 0: {e}")
        return 0


def sync_delta_to_timescaledb():
    """Recovery sync — catches any batches that failed the foreachBatch fast path."""
    logging.info("Sync loop: checking for unsynced records...")
    last_ts = get_last_synced_ts()
    logging.info(f"Sync loop: last synced timestamp = {last_ts}")

    df = (
        spark.read.format("delta")
        .load(DELTA_PATH)
        .filter(col("collection_time") > last_ts)
        .withColumn("time", from_unixtime(col("collection_time")).cast("timestamp"))
        .select(*TIMESCALE_COLUMNS)
    )

    if df.limit(1).count() == 0:
        logging.info("Sync loop: nothing to sync")
        return

    # write_to_timescale uses ON CONFLICT DO NOTHING — safe even if fast path
    # already wrote these records
    write_to_timescale(df, label="sync_loop")
    logging.info("Sync loop: sync complete")


def sync_loop():
    """Runs every 5 minutes as a recovery net for failed foreachBatch writes."""
    while True:
        time.sleep(300)
        try:
            sync_delta_to_timescaledb()
        except Exception as e:
            logging.error(f"Sync loop error: {e}")


# ── foreachBatch: dual write (Delta + TimescaleDB) ────────────────────────────
def upsert_to_delta_and_timescale(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    deduped_df = (
        batch_df.dropDuplicates(["device_sn", "collection_time"])
        .withColumn("time", from_unixtime(col("collection_time")).cast("timestamp"))
        .cache()
    )

    # ── Write 1: Delta Lake (ACID, source of truth) ────────────────────────────
    delta_table = DeltaTable.forPath(spark, DELTA_PATH)
    delta_table.alias("target").merge(
        deduped_df.alias("source"),
        "target.device_sn = source.device_sn "
        "AND target.collection_time = source.collection_time",
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    # ── Write 2: TimescaleDB fast path (idempotent via staging table) ──────────
    try:
        write_to_timescale(deduped_df, label=f"batch_{batch_id}")
    except Exception as e:
        logging.error(
            f"Batch {batch_id}: TimescaleDB write failed — sync_loop will recover: {e}"
        )
    finally:
        deduped_df.unpersist()


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

# Sync backfill to TimescaleDB (idempotent — safe to re-run)
sync_delta_to_timescaledb()

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
    .option("checkpointLocation", f"{DELTA_PATH}/_checkpoints/realtime")
    .trigger(
        processingTime="30 seconds"
    )  # balanced: headroom for Delta merge + JDBC write
    .start()
)

logging.info("- All streaming queries successfully initiated.")

# Recovery sync thread — runs every 5 min, catches any failed fast-path writes
sync_thread = threading.Thread(target=sync_loop, daemon=True)
sync_thread.start()

realtime_delta_query.awaitTermination()
