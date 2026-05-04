from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def print_bronze_aisles_context() -> None:
    print("==================================================")
    print("Retail Lakehouse - Contexto Bronze aisles")
    print(f"LOCAL_RAW_DIR={os.getenv('LOCAL_RAW_DIR')}")
    print(f"LOCAL_BRONZE_DIR={os.getenv('LOCAL_BRONZE_DIR')}")
    print("Objetivo: transformar raw de aisles em Bronze Parquet")
    print("Execução real do job: container Spark")
    print("==================================================")


with DAG(
    dag_id="dag_bronze_aisles",
    description="Transformação Bronze da entidade aisles com PySpark",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    tags=["retail-lakehouse", "bronze", "spark", "aisles"],
) as dag:

    start = EmptyOperator(task_id="start")

    show_context = PythonOperator(
        task_id="show_bronze_aisles_context",
        python_callable=print_bronze_aisles_context,
    )

    run_bronze_aisles = BashOperator(
        task_id="run_bronze_aisles",
        bash_command=(
            "docker exec retail_lakehouse_spark "
            "/opt/spark/bin/spark-submit "
            "--master local[*] "
            "/opt/spark-apps/jobs/bronze/bronze_aisles.py"
        ),
    )

    end = EmptyOperator(task_id="end")

    start >> show_context >> run_bronze_aisles >> end