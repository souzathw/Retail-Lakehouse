from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def print_silver_products_enriched_context() -> None:
    print("==================================================")
    print("Retail Lakehouse - Contexto Silver products_enriched")
    print(f"LOCAL_BRONZE_DIR={os.getenv('LOCAL_BRONZE_DIR')}")
    print(f"LOCAL_SILVER_DIR={os.getenv('LOCAL_SILVER_DIR')}")
    print("Objetivo: enriquecer products com aisles e departments")
    print("Execução real do job: container Spark")
    print("==================================================")


with DAG(
    dag_id="dag_silver_products_enriched",
    description="Transformação Silver products_enriched com PySpark",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    tags=["retail-lakehouse", "silver", "spark", "products-enriched"],
) as dag:

    start = EmptyOperator(task_id="start")

    show_context = PythonOperator(
        task_id="show_silver_products_enriched_context",
        python_callable=print_silver_products_enriched_context,
    )

    run_silver = BashOperator(
        task_id="run_silver_products_enriched",
        bash_command=(
            "docker exec retail_lakehouse_spark "
            "/opt/spark/bin/spark-submit "
            "--master local[*] "
            "/opt/spark-apps/jobs/silver/silver_products_enriched.py"
        ),
    )

    end = EmptyOperator(task_id="end")

    start >> show_context >> run_silver >> end