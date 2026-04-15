from __future__ import annotations

import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator


AIRFLOW_INGESTION_PATH = "/opt/airflow/ingestion/src"

if AIRFLOW_INGESTION_PATH not in sys.path:
    sys.path.append(AIRFLOW_INGESTION_PATH)

from ingest_instacart_local import run_local_raw_ingestion 

def print_ingestion_context() -> None:

    print("==================================================")
    print("Retail Lakehouse - DAG de ingestão local")
    print("Objetivo: copiar o dataset Instacart para a camada raw local")
    print(f"LOCAL_SOURCE_INSTACART_DIR={os.getenv('LOCAL_SOURCE_INSTACART_DIR')}")
    print(f"LOCAL_RAW_DIR={os.getenv('LOCAL_RAW_DIR')}")
    print("==================================================")


with DAG(
    dag_id="dag_ingest_instacart_local",
    description="Ingestão local do dataset Instacart para a camada raw",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    tags=["retail-lakehouse", "ingestion", "raw", "instacart"],
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    show_context = PythonOperator(
        task_id="show_ingestion_context",
        python_callable=print_ingestion_context,
    )

    ingest_to_raw = PythonOperator(
        task_id="ingest_instacart_to_local_raw",
        python_callable=run_local_raw_ingestion,
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> show_context >> ingest_to_raw >> end