from __future__ import annotations

import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


AIRFLOW_INGESTION_PATH = "/opt/airflow/ingestion/src"

if AIRFLOW_INGESTION_PATH not in sys.path:
    sys.path.append(AIRFLOW_INGESTION_PATH)


def print_validate_bronze_products_context() -> None:
    print("==================================================")
    print("Retail Lakehouse - Contexto validação Bronze products")
    print(f"LOCAL_BRONZE_DIR={os.getenv('LOCAL_BRONZE_DIR')}")
    print("Objetivo: validar a saída Parquet da Bronze de products")
    print("==================================================")


def run_validate_bronze_products() -> None:
    from validate_bronze_products import validate_bronze_products
    validate_bronze_products()


with DAG(
    dag_id="dag_validate_bronze_products",
    description="Validação da Bronze da entidade products",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    tags=["retail-lakehouse", "bronze", "validation", "products"],
) as dag:

    start = EmptyOperator(task_id="start")

    show_context = PythonOperator(
        task_id="show_validate_bronze_products_context",
        python_callable=print_validate_bronze_products_context,
    )

    validate_bronze = PythonOperator(
        task_id="validate_bronze_products",
        python_callable=run_validate_bronze_products,
    )

    end = EmptyOperator(task_id="end")

    start >> show_context >> validate_bronze >> end