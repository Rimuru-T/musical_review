from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import subprocess
import psycopg2
import os
from kaggle.api.kaggle_api_extended import KaggleApi
import json

default_args = {
    'owner': 'airflow',
}


def download_dataset():
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files('eswarchandt/amazon-music-revews', path='./GLUE/data', unzip=True)


def create_schemas():
    conn = psycopg2.connect(
        host="postgres",
        database="music_reviews",
        user="root",
        password="5340"
    )
    cur = conn.cursor()

    cur.execute ("CREATE SCHEMA IF NOT EXISTS raw;")
    cur.execute("CREATE SCHEMA IF NOT EXISTS staging;")
    cur.execute("CREATE SCHEMA IF NOT EXISTS core;")
    cur.execute("CREATE SCHEMA IF NOT EXISTS datamart;")

    '''cur.execute("""
    CREATE OR REPLACE FUNCTION datamart.sum_helpful(arr text[]) RETURNS text AS $$
    DECLARE
    total point := point(0,0);
    current point;
    val text;
    coords float8[];
    BEGIN
    FOREACH val IN ARRAY arr LOOP
        coords := regexp_split_to_array(trim(both '[]' from val), '\s*,\s*')::float8[];
        current := point(coords[1], coords[2]);
        total := point(total[0] + current[0], total[1] + current[1]);
    END LOOP;

    RETURN format('[%s, %s]', total[0], total[1]);
    END;
    $$ LANGUAGE plpgsql IMMUTABLE STRICT;
    """)'''

    conn.commit()
    cur.close()
    conn.close()


def load_staging():
    conn = psycopg2.connect(
        host="postgres",
        database="music_reviews",
        user="root",
        password="5340"
    )
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS raw.staging_reviews;")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS raw.staging_reviews (
        reviewerID TEXT, 
        asin TEXT, 
        reviewerName TEXT, 
        helpful TEXT, 
        reviewText TEXT, 
        overall float8, 
        summary TEXT, 
        unixReviewTime TEXT, 
        reviewTime TEXT

    );
    """)
    csv_path = "/home/airflow/data/Musical_Instruments_reviews.csv"
    with open(csv_path, "r", encoding="utf-8") as f:
        cur.copy_expert("COPY raw.staging_reviews FROM STDIN WITH CSV HEADER DELIMITER AS ',';", f)
    conn.commit()
    cur.close()
    conn.close()

def run_dbt():
    subprocess.run(["dbt", "run", "--project-dir", "/home/airflow/dbt_project", "--profiles-dir", "/home/airflow/dbt_project"], check=True)


with DAG(
    "music_reviews_etl",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    #t1 = PythonOperator(task_id="download_dataset", python_callable=download_dataset)
    t1 = PythonOperator(task_id="create_schemas", python_callable=create_schemas)
    t2 = PythonOperator(task_id="load_staging", python_callable=load_staging)
    t3 = PythonOperator(task_id="run_dbt", python_callable=run_dbt)

    t1 >> t2 >> t3
