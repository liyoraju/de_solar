# Dockerfile (same folder as docker-compose.yaml)
FROM apache/airflow:3.2.2

RUN pip install apache-airflow==3.2.2 \
    apache-airflow-providers-apache-spark==4.10.0 \
    dbt-core==1.10.4 \
    dbt-duckdb==1.9.1 \
    duckdb==1.2.2 \
    great-expectations==0.18.0 \
    requests==2.32.0