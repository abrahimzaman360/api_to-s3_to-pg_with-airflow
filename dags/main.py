import sys
import os

# Add the parent directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from functions import load_to_pg, load_to_s3

with DAG(
    dag_id="git-pg-etl",
    schedule="*/5 * * * *",  # every 5 minutes
    start_date=datetime(2025, 5, 14),
    catchup=False,
    tags=["git", "postgres", "etl"],
) as dag:
    
    load_to_s3_task = PythonOperator(
        task_id="load_to_s3",
        python_callable=load_to_s3.fetch_and_upload_to_s3,
        op_kwargs={"user": "abrahimzaman360"},
    )

    load_to_postgres_task = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_pg.load_to_postgres,
    )

    load_to_s3_task >> load_to_postgres_task
