from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def print_gold_top_products_context() -> None:
    print("==================================================")
    print("Retail Lakehouse - Contexto Gold top_products")
    print(f"LOCAL_SILVER_DIR={os.getenv('LOCAL_SILVER_DIR')}")
    print(f"LOCAL_GOLD_DIR={os.getenv('LOCAL_GOLD_DIR')}")
    print("Objetivo: agregar métricas de volume e recompra por produto")
    print("Execução real do job: container Spark")
    print("==================================================")


with DAG(
    dag_id="dag_gold_top_products",
    description="Transformação Gold top_products com PySpark",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    tags=["retail-lakehouse", "gold", "spark", "top-products"],
) as dag:

    start = EmptyOperator(task_id="start")

    show_context = PythonOperator(
        task_id="show_gold_top_products_context",
        python_callable=print_gold_top_products_context,
    )

    run_gold = BashOperator(
        task_id="run_gold_top_products",
        bash_command=(
            "docker exec retail_lakehouse_spark "
            "/opt/spark/bin/spark-submit --master local[*] "
            "/opt/spark-apps/jobs/gold/gold_top_products.py"
        ),
    )

    end = EmptyOperator(task_id="end")

    start >> show_context >> run_gold >> end