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


def print_prepare_source_context() -> None:
    print("==================================================")
    print("Retail Lakehouse - Preparação da origem via Kaggle")
    print(f"KAGGLE_DATASET={os.getenv('KAGGLE_DATASET')}")
    print(f"LOCAL_SOURCE_INSTACART_DIR={os.getenv('LOCAL_SOURCE_INSTACART_DIR')}")
    print("==================================================")


def run_prepare_instacart_source_from_kaggle() -> None:
    from download_instacart_kaggle import prepare_instacart_source_from_kaggle

    prepare_instacart_source_from_kaggle()


with DAG(
    dag_id="dag_prepare_instacart_source",
    description="Baixa e prepara a origem local do dataset Instacart via Kaggle",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    tags=["retail-lakehouse", "kaggle", "source", "instacart"],
) as dag:

    start = EmptyOperator(task_id="start")

    show_context = PythonOperator(
        task_id="show_prepare_source_context",
        python_callable=print_prepare_source_context,
    )

    prepare_source = PythonOperator(
        task_id="prepare_instacart_source_from_kaggle",
        python_callable=run_prepare_instacart_source_from_kaggle,
    )

    end = EmptyOperator(task_id="end")

    start >> show_context >> prepare_source >> end