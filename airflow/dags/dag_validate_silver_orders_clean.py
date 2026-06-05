from __future__ import annotations

import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


AIRFLOW_INGESTION_PATH = "/opt/airflow/ingestion/src"


def print_validate_silver_orders_clean_context() -> None:
    print("==================================================")
    print("Retail Lakehouse - Contexto validação Silver orders_clean")
    print(f"LOCAL_SILVER_DIR={os.getenv('LOCAL_SILVER_DIR')}")
    print("Objetivo: validar a saída Parquet da Silver de orders_clean")
    print("==================================================")


def run_validate_silver_orders_clean_wrapper() -> None:
    if AIRFLOW_INGESTION_PATH not in sys.path:
        sys.path.append(AIRFLOW_INGESTION_PATH)

    from validate_silver_orders_clean import validate_silver_orders_clean

    validate_silver_orders_clean()


with DAG(
    dag_id="dag_validate_silver_orders_clean",
    description="Validação da Silver da entidade orders_clean",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    tags=["retail-lakehouse", "silver", "validation", "orders-clean"],
) as dag:

    start = EmptyOperator(task_id="start")

    show_context = PythonOperator(
        task_id="show_validate_silver_orders_clean_context",
        python_callable=print_validate_silver_orders_clean_context,
    )

    validate_silver = PythonOperator(
        task_id="validate_silver_orders_clean",
        python_callable=run_validate_silver_orders_clean_wrapper,
    )

    end = EmptyOperator(task_id="end")

    start >> show_context >> validate_silver >> end