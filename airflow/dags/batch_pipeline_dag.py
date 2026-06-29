"""Batch pipeline: Bronze → (DQ) → Silver → Gold facts.

Runs every 2 hours, window-scoped and idempotent. Each task submits a SparkApplication
to the Spark Operator (namespace `spark`) via SparkKubernetesOperator; the manifest is
rendered from the template + job config on S3 by `lib.spark_application.render`, with the
data interval injected as `{{ data_interval_start/end }}` macros.
"""
from __future__ import annotations

from datetime import datetime, timedelta

import yaml
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)

from lib.spark_application import render

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _spark_task(dag: DAG, task_id: str, job_name: str) -> SparkKubernetesOperator:
    return SparkKubernetesOperator(
        task_id=task_id,
        namespace="spark",
        template_spec=yaml.safe_load(render(job_name)),
        kubernetes_conn_id="kubernetes_default",
        get_logs=True,
        delete_on_termination=True,
        dag=dag,
    )


with DAG(
    dag_id="batch_pipeline",
    description="Bronze → Silver → Gold facts (Iceberg), Airflow-orchestrated",
    start_date=datetime(2026, 6, 1),
    schedule="0 */2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["batch", "iceberg", "logistics"],
) as dag:
    ensure_tables = _spark_task(dag, "ensure_tables", "batch_ensure_tables")
    silver_build = _spark_task(dag, "silver_build", "silver_build")
    gold_build = _spark_task(dag, "gold_build", "gold_build")

    ensure_tables >> silver_build >> gold_build
