FROM apache/airflow:2.10.5

RUN python -m venv /home/airflow/dbt_venv \
 && /home/airflow/dbt_venv/bin/pip install --no-cache-dir \
    dbt-core==1.9.0 dbt-postgres==1.9.0 \
    dbt-adapters==1.16.3 dbt-common==1.27.1
