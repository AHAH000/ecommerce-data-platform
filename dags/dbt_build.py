from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DBT = "cd /opt/airflow/dbt_project && /home/airflow/dbt_venv/bin/dbt"
DBT_ENV = {"DBT_TARGET_PATH": "/tmp/dbt_target", "DBT_LOG_PATH": "/tmp/dbt_logs"}

with DAG(
    dag_id="dbt_build",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["olist", "dbt"],
) as dag:
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"{DBT} run",
        env=DBT_ENV,
        append_env=True,
    )
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT} test",
        env=DBT_ENV,
        append_env=True,
    )
    dbt_run >> dbt_test
