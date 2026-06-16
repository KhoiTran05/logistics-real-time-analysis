from airflow import DAG
from datetime import datetime

dag = DAG(
    dag_id="empty_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
)