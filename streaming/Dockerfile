FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root

RUN pip install --no-cache-dir \
    delta-spark==3.2.0

ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.1/postgresql-42.7.1.jar \
    /opt/spark/jars/ext/postgresql-42.7.1.jar

RUN mkdir -p /tmp/.ivy2/cache /tmp/.ivy2/jars \
    && chown -R spark:spark /tmp/.ivy2 /opt/spark/jars/ext

USER spark

ENV SPARK_SUBMIT_OPTS="-Divy.home=/tmp/.ivy2"

CMD ["/opt/spark/bin/spark-submit", \
     "--master", "local[2]", \
     "--driver-memory", "1g", \
     "--jars", "/opt/spark/jars/ext/postgresql-42.7.1.jar", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,io.delta:delta-spark_2.12:3.2.0", \
     "--conf", "spark.driver.maxResultSize=512m", \
     "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension", \
     "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog", \
     "/app/main.py"]
