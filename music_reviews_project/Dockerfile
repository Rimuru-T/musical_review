FROM apache/airflow:2.7.1-python3.8

# Устанавливаем нужные зависимости
USER root

RUN apt-get update && \
    apt-get install -y build-essential && \
    apt-get clean

USER airflow

# Устанавливаем Python-пакеты
RUN pip install --no-cache-dir \
    kaggle \
    dbt-core==1.8.7 \
    dbt-postgres==1.8.2 \
    psycopg2-binary
