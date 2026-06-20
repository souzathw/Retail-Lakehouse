from __future__ import annotations

import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


AIRFLOW_INGESTION_PATH = "/opt/airflow/ingestion/src"


def print_validate_gold_top_products_context() -> None:
    print("==================================================")
    print("Retail Lakehouse - Contexto validação Gold top_products")
    print(f"LOCAL_GOLD_DIR={os.getenv('LOCAL_GOLD_DIR')}")
    print("Objetivo: validar a saída Parquet da Gold de top_products")
    print("==================================================")


def run_validate_gold_top_products_wrapper() -> None:
    if AIRFLOW_INGESTION_PATH not in sys.path:
        sys.path.append(AIRFLOW_INGESTION_PATH)

    from validate_gold_top_products import validate_gold_top_products

    validate_gold_top_products()


with DAG(
    dag_id="dag_validate_gold_top_products",
    description="Validação da Gold da entidade top_products",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    tags=["retail-lakehouse", "gold", "validation", "top-products"],
) as dag:

    start = EmptyOperator(task_id="start")

    show_context = PythonOperator(
        task_id="show_validate_gold_top_products_context",
        python_callable=print_validate_gold_top_products_context,
    )

    validate_gold = PythonOperator(
        task_id="validate_gold_top_products",
        python_callable=run_validate_gold_top_products_wrapper,
    )

    end = EmptyOperator(task_id="end")

    start >> show_context >> validate_gold >> end