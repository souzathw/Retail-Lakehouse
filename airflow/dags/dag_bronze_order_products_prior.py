from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def print_bronze_order_products_prior_context() -> None:
    print("==================================================")
    print("Retail Lakehouse - Contexto Bronze order_products_prior")
    print(f"LOCAL_RAW_DIR={os.getenv('LOCAL_RAW_DIR')}")
    print(f"LOCAL_BRONZE_DIR={os.getenv('LOCAL_BRONZE_DIR')}")
    print("Objetivo: transformar raw de order_products_prior em Bronze Parquet")
    print("Execução real do job: container Spark")
    print("==================================================")


with DAG(
    dag_id="dag_bronze_order_products_prior",
    description="Transformação Bronze da entidade order_products_prior com PySpark",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    tags=["retail-lakehouse", "bronze", "spark", "order-products-prior"],
) as dag:

    start = EmptyOperator(task_id="start")

    show_context = PythonOperator(
        task_id="show_bronze_order_products_prior_context",
        python_callable=print_bronze_order_products_prior_context,
    )

    run_bronze = BashOperator(
        task_id="run_bronze_order_products_prior",
        bash_command=(
            "docker exec retail_lakehouse_spark "
            "/opt/spark/bin/spark-submit "
            "--master local[*] "
            "/opt/spark-apps/jobs/bronze/bronze_order_products_prior.py"
        ),
    )

    end = EmptyOperator(task_id="end")

    start >> show_context >> run_bronze >> end