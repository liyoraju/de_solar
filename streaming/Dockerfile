# streaming/Dockerfile
FROM apache/spark:3.5.3-python3

USER root

RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

# install delta lake + kafka connector
RUN pip install delta-spark==3.2.0 pyspark==3.5.3

# download kafka connector jar
RUN curl -o /opt/spark/jars/spark-sql-kafka.jar \
  https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar

# download delta lake jars (for when we use Spark to read/write Delta Lake)
RUN curl -o /opt/spark/jars/delta-spark.jar \
  https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar && \
  curl -o /opt/spark/jars/delta-storage.jar \
  https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar

USER spark

# Start Spark master and keep container alive for submitting jobs
CMD ["/bin/bash", "-c", "/opt/spark/sbin/start-master.sh && sleep infinity"]