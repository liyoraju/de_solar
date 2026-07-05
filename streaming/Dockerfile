# streaming/Dockerfile
FROM apache/spark:3.5.3-python3

USER root

RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

# install delta lake + kafka connector
RUN pip install delta-spark==3.2.0 pyspark==3.5.3

USER spark

# Start Spark master and keep container alive for submitting jobs
CMD ["/bin/bash", "-c", "\
if [ \"$SPARK_MODE\" = 'worker' ]; then \
  /opt/spark/sbin/start-worker.sh $SPARK_MASTER_URL; \
else \
  /opt/spark/sbin/start-master.sh; \
  fi && sleep infinity"]