from __future__ import annotations

import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


AIRFLOW_INGESTION_PATH = "/opt/airflow/ingestion/src"


def print_validate_silver_order_items_train_enriched_context() -> None:
    print("==================================================")
    print("Retail Lakehouse - Contexto validação Silver order_items_train_enriched")
    print(f"LOCAL_SILVER_DIR={os.getenv('LOCAL_SILVER_DIR')}")
    print("Objetivo: validar a saída Parquet da Silver de order_items_train_enriched")
    print("==================================================")


def run_validate_silver_order_items_train_enriched_wrapper() -> None:
    if AIRFLOW_INGESTION_PATH not in sys.path:
        sys.path.append(AIRFLOW_INGESTION_PATH)

    from validate_silver_order_items_train_enriched import (
        validate_silver_order_items_train_enriched,
    )

    validate_silver_order_items_train_enriched()


with DAG(
    dag_id="dag_validate_silver_order_items_train_enriched",
    description="Validação da Silver da entidade order_items_train_enriched",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    tags=["retail-lakehouse", "silver", "validation", "order-items-train-enriched"],
) as dag:

    start = EmptyOperator(task_id="start")

    show_context = PythonOperator(
        task_id="show_validate_silver_order_items_train_enriched_context",
        python_callable=print_validate_silver_order_items_train_enriched_context,
    )

    validate_silver = PythonOperator(
        task_id="validate_silver_order_items_train_enriched",
        python_callable=run_validate_silver_order_items_train_enriched_wrapper,
    )

    end = EmptyOperator(task_id="end")

    start >> show_context >> validate_silver >> end