from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_bronze_departments_context() -> None:
    print("==================================================")
    print("Retail Lakehouse - Contexto Bronze departments")
    print(f"LOCAL_RAW_DIR={os.getenv('LOCAL_RAW_DIR')}")
    print(f"LOCAL_BRONZE_DIR={os.getenv('LOCAL_BRONZE_DIR')}")
    print(f"SPARK_MASTER={os.getenv('SPARK_MASTER')}")
    print("Objetivo: transformar raw de departments em Bronze Parquet")
    print("Execução atual: spark-submit via container Spark")
    print("==================================================")


with DAG(
    dag_id="dag_bronze_departments",
    description="Transformação Bronze da entidade departments com PySpark",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    tags=["retail-lakehouse", "bronze", "spark", "departments"],
) as dag:

    start = EmptyOperator(task_id="start")

    show_context = PythonOperator(
        task_id="show_bronze_departments_context",
        python_callable=print_bronze_departments_context,
    )

    run_bronze_departments = BashOperator(
        task_id="run_bronze_departments",
        bash_command=(
            "docker exec retail_lakehouse_spark "
            "/opt/spark/bin/spark-submit "
            "--master local[*] "
            "/opt/spark-apps/jobs/bronze/bronze_departments.py"
        ),
    )

    end = EmptyOperator(task_id="end")

    start >> show_context >> run_bronze_departments >> end