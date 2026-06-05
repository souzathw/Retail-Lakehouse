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


def print_validate_silver_products_enriched_context() -> None:
    print("==================================================")
    print("Retail Lakehouse - Contexto validação Silver products_enriched")
    print(f"LOCAL_SILVER_DIR={os.getenv('LOCAL_SILVER_DIR')}")
    print("Objetivo: validar a saída Parquet da Silver de products_enriched")
    print("==================================================")


def run_validate_silver_products_enriched() -> None:
    from validate_silver_products_enriched import validate_silver_products_enriched

    validate_silver_products_enriched()


with DAG(
    dag_id="dag_validate_silver_products_enriched",
    description="Validação da Silver da entidade products_enriched",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    tags=["retail-lakehouse", "silver", "validation", "products-enriched"],
) as dag:

    start = EmptyOperator(task_id="start")

    show_context = PythonOperator(
        task_id="show_validate_silver_products_enriched_context",
        python_callable=print_validate_silver_products_enriched_context,
    )

    validate_silver = PythonOperator(
        task_id="validate_silver_products_enriched",
        python_callable=run_validate_silver_products_enriched,
    )

    end = EmptyOperator(task_id="end")

    start >> show_context >> validate_silver >> end