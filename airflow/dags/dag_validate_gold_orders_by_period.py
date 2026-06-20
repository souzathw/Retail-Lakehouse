from __future__ import annotations

import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


AIRFLOW_INGESTION_PATH = "/opt/airflow/ingestion/src"


def print_validate_gold_orders_by_period_context() -> None:
    print("==================================================")
    print("Retail Lakehouse - Contexto validação Gold orders_by_period")
    print(f"LOCAL_GOLD_DIR={os.getenv('LOCAL_GOLD_DIR')}")
    print("Objetivo: validar a saída Parquet da Gold de orders_by_period")
    print("==================================================")


def run_validate_gold_orders_by_period_wrapper() -> None:
    if AIRFLOW_INGESTION_PATH not in sys.path:
        sys.path.append(AIRFLOW_INGESTION_PATH)

    from validate_gold_orders_by_period import validate_gold_orders_by_period

    validate_gold_orders_by_period()


with DAG(
    dag_id="dag_validate_gold_orders_by_period",
    description="Validação da Gold da entidade orders_by_period",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    tags=["retail-lakehouse", "gold", "validation", "orders-by-period"],
) as dag:

    start = EmptyOperator(task_id="start")

    show_context = PythonOperator(
        task_id="show_validate_gold_orders_by_period_context",
        python_callable=print_validate_gold_orders_by_period_context,
    )

    validate_gold = PythonOperator(
        task_id="validate_gold_orders_by_period",
        python_callable=run_validate_gold_orders_by_period_wrapper,
    )

    end = EmptyOperator(task_id="end")

    start >> show_context >> validate_gold >> end