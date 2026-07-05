from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col

spark = (
    SparkSession.builder.appName("solar-pipeline")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
        "io.delta:delta-spark_2.12:3.2.0",
    )
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.streaming.checkpointLocation", "/data/delta/checkpoints")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.shuffle.partitions", "4")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)

spark.readStream("kafka").option()
