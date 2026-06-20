from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def print_gold_orders_by_period_context() -> None:
    print("==================================================")
    print("Retail Lakehouse - Contexto Gold orders_by_period")
    print(f"LOCAL_SILVER_DIR={os.getenv('LOCAL_SILVER_DIR')}")
    print(f"LOCAL_GOLD_DIR={os.getenv('LOCAL_GOLD_DIR')}")
    print("Objetivo: agregar pedidos por dia da semana e hora do dia")
    print("Execução real do job: container Spark")
    print("==================================================")


with DAG(
    dag_id="dag_gold_orders_by_period",
    description="Transformação Gold orders_by_period com PySpark",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    tags=["retail-lakehouse", "gold", "spark", "orders-by-period"],
) as dag:

    start = EmptyOperator(task_id="start")

    show_context = PythonOperator(
        task_id="show_gold_orders_by_period_context",
        python_callable=print_gold_orders_by_period_context,
    )

    run_gold = BashOperator(
        task_id="run_gold_orders_by_period",
        bash_command=(
            "docker exec retail_lakehouse_spark "
            "/opt/spark/bin/spark-submit --master local[*] "
            "/opt/spark-apps/jobs/gold/gold_orders_by_period.py"
        ),
    )

    end = EmptyOperator(task_id="end")

    start >> show_context >> run_gold >> end