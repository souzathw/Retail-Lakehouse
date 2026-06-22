from __future__ import annotations

import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


AIRFLOW_INGESTION_PATH = "/opt/airflow/ingestion/src"


def print_validate_gold_repurchase_metrics_context() -> None:
    print("==================================================")
    print("Retail Lakehouse - Contexto validação Gold repurchase_metrics")
    print(f"LOCAL_GOLD_DIR={os.getenv('LOCAL_GOLD_DIR')}")
    print("Objetivo: validar a saída Parquet da Gold de repurchase_metrics")
    print("==================================================")


def run_validate_gold_repurchase_metrics_wrapper() -> None:
    if AIRFLOW_INGESTION_PATH not in sys.path:
        sys.path.append(AIRFLOW_INGESTION_PATH)

    from validate_gold_repurchase_metrics import validate_gold_repurchase_metrics

    validate_gold_repurchase_metrics()


with DAG(
    dag_id="dag_validate_gold_repurchase_metrics",
    description="Validação da Gold da entidade repurchase_metrics",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    tags=["retail-lakehouse", "gold", "validation", "repurchase-metrics"],
) as dag:

    start = EmptyOperator(task_id="start")

    show_context = PythonOperator(
        task_id="show_validate_gold_repurchase_metrics_context",
        python_callable=print_validate_gold_repurchase_metrics_context,
    )

    validate_gold = PythonOperator(
        task_id="validate_gold_repurchase_metrics",
        python_callable=run_validate_gold_repurchase_metrics_wrapper,
    )

    end = EmptyOperator(task_id="end")

    start >> show_context >> validate_gold >> end