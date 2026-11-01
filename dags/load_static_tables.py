import csv
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DATA_DIR = Path("/opt/airflow/data")

STATIC_TABLES = {
    "customers": "olist_customers_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}


def load_static(table: str, filename: str) -> None:
    path = DATA_DIR / filename

    with open(path, encoding="utf-8-sig", newline="") as f:
        header = next(csv.reader(f))
    columns = ", ".join(f'"{c}" TEXT' for c in header)

    conn = PostgresHook(postgres_conn_id="warehouse").get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS raw")
            cur.execute(f'DROP TABLE IF EXISTS raw."{table}"')
            cur.execute(f'CREATE TABLE raw."{table}" ({columns})')
            with open(path, encoding="utf-8-sig") as f:
                cur.copy_expert(
                    f'COPY raw."{table}" FROM STDIN WITH (FORMAT csv, HEADER true)', f
                )
    finally:
        conn.close()


with DAG(
    dag_id="load_static_tables",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["olist", "raw"],
) as dag:
    for table, filename in STATIC_TABLES.items():
        PythonOperator(
            task_id=f"load_{table}",
            python_callable=load_static,
            op_kwargs={"table": table, "filename": filename},
        )