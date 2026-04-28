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

from validate_instacart_raw import validate_instacart_raw  # noqa: E402


def print_raw_validation_context() -> None:
    print("==================================================")
    print("Retail Lakehouse - Contexto da validação raw")
    print(f"LOCAL_RAW_DIR={os.getenv('LOCAL_RAW_DIR')}")
    print("Objetivo: validar arquivos, partições, linhas e colunas da raw")
    print("==================================================")


with DAG(
    dag_id="dag_validate_instacart_raw",
    description="Validação da camada raw do dataset Instacart",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    tags=["retail-lakehouse", "validation", "raw", "instacart"],
) as dag:

    start = EmptyOperator(task_id="start")

    show_context = PythonOperator(
        task_id="show_raw_validation_context",
        python_callable=print_raw_validation_context,
    )

    validate_raw = PythonOperator(
        task_id="validate_instacart_raw",
        python_callable=validate_instacart_raw,
    )

    end = EmptyOperator(task_id="end")

    start >> show_context >> validate_raw >> end