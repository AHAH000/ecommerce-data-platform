import io
import logging
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

DATA_DIR = Path("/opt/airflow/data")
ORDERS_FILE = "olist_orders_dataset.csv"

TABLES = {
    "orders": ORDERS_FILE,
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
}


def notify_failure(context) -> None:
    ti = context["task_instance"]
    log.error(
        "PIPELINE FAILURE dag=%s task=%s run=%s try=%s",
        ti.dag_id, ti.task_id, context["run_id"], ti.try_number,
    )


def create_tables() -> None:
    conn = PostgresHook(postgres_conn_id="warehouse").get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS raw")
            for table, filename in TABLES.items():
                header = pd.read_csv(
                    DATA_DIR / filename, nrows=0, dtype=str, encoding="utf-8-sig"
                ).columns
                cols = ", ".join(f'"{c}" TEXT' for c in header)
                cur.execute(
                    f'CREATE TABLE IF NOT EXISTS raw."{table}" ({cols}, _purchase_date DATE)'
                )
    finally:
        conn.close()


def load_slice(table: str, filename: str, ds: str) -> None:
    read = lambda f: pd.read_csv(
        DATA_DIR / f, dtype=str, keep_default_na=False, encoding="utf-8-sig"
    )
    orders = read(ORDERS_FILE)
    day_orders = orders[orders["order_purchase_timestamp"].str[:10] == ds]

    if table == "orders":
        df = day_orders.copy()
    else:
        child = read(filename)
        df = child[child["order_id"].isin(day_orders["order_id"])].copy()
    df["_purchase_date"] = ds

    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False)
    buf.seek(0)

    conn = PostgresHook(postgres_conn_id="warehouse").get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(f'DELETE FROM raw."{table}" WHERE _purchase_date = %s', (ds,))
            if len(df) > 0:
                cur.copy_expert(f'COPY raw."{table}" FROM STDIN WITH (FORMAT csv)', buf)
    finally:
        conn.close()
    print(f"{table}: loaded {len(df)} rows for {ds}")


def check_quality(ds: str) -> None:
    hook = PostgresHook(postgres_conn_id="warehouse")
    orphan = """
        SELECT count(*) FROM raw.{t} c
        WHERE c._purchase_date = %s
          AND NOT EXISTS (
              SELECT 1 FROM raw.orders o
              WHERE o.order_id = c.order_id AND o._purchase_date = %s)
    """
    checks = {
        "duplicate order_id in orders": (
            """SELECT count(*) FROM (
                   SELECT order_id FROM raw.orders
                   WHERE _purchase_date = %s
                   GROUP BY order_id HAVING count(*) > 1) d""",
            (ds,),
        ),
        "null or empty order_id in orders": (
            """SELECT count(*) FROM raw.orders
               WHERE _purchase_date = %s AND (order_id IS NULL OR order_id = '')""",
            (ds,),
        ),
        "orphan order_items": (orphan.format(t="order_items"), (ds, ds)),
        "orphan order_payments": (orphan.format(t="order_payments"), (ds, ds)),
        "orphan order_reviews": (orphan.format(t="order_reviews"), (ds, ds)),
    }
    failures = []
    for name, (sql, params) in checks.items():
        bad = hook.get_first(sql, parameters=params)[0]
        log.info("check '%s': %s bad rows", name, bad)
        if bad:
            failures.append(f"{name}: {bad}")
    if failures:
        raise ValueError(f"Data quality failed for {ds}: " + "; ".join(failures))


with DAG(
    dag_id="load_daily_orders",
    start_date=datetime(2016, 9, 4),
    end_date=datetime(2018, 10, 17),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
        "retry_exponential_backoff": True,
        "on_failure_callback": notify_failure,
    },
    tags=["olist", "raw"],
) as dag:
    setup = PythonOperator(task_id="create_tables", python_callable=create_tables)

    quality = PythonOperator(
        task_id="data_quality_checks",
        python_callable=check_quality,
        op_kwargs={"ds": "{{ ds }}"},
        retries=0,  # a deterministic check gives the same answer on retry
    )

    for table, filename in TABLES.items():
        load = PythonOperator(
            task_id=f"load_{table}",
            python_callable=load_slice,
            op_kwargs={"table": table, "filename": filename, "ds": "{{ ds }}"},
        )
        setup >> load >> quality