from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/opt/airflow/project"
PYTHON = "python"  

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="crypto_hourly_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["crypto", "cmc"],
) as dag:

    ingest = BashOperator(
        task_id="ingest_cmc",
        bash_command=f"cd {PROJECT_DIR} && {PYTHON} ingest_cmc.py",
        env={**os.environ},
    )

    compute = BashOperator(
        task_id="spark_hourly_change",
        bash_command=f"cd {PROJECT_DIR} && {PYTHON} spark_hourly_change.py",
        env={**os.environ},
    )

    dbt_hourly_gainers = BashOperator(
        task_id="dbt_hourly_gainers",
        bash_command=(
            "cd /opt/airflow/project/dbt && dbt deps && dbt run --select crypto_hourly_gainers"
        ),
        env={**os.environ, "DBT_PROFILES_DIR": "/opt/airflow/project/dbt"},
    )

    ingest >> compute >> dbt_hourly_gainers