from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator


def print_project_context() -> None:
    print("==============================================")
    print("Retail Lakehouse - DAG de validação inicial")
    print("Objetivo: confirmar que o Airflow está funcional")
    print("Ambiente local: OK")
    print("Scheduler: executando")
    print("Webserver: executando")
    print("Task Python: executada com sucesso")
    print("==============================================")


def print_next_steps() -> None:
    print("Próximos passos do projeto:")
    print("1. Validar leitura de arquivos pelo Airflow")
    print("2. Criar DAG de ingestão do dataset Instacart")
    print("3. Enviar dados para camada raw")
    print("4. Iniciar processamento bronze com PySpark")


with DAG(
    dag_id="dag_hello_world",
    description="DAG inicial para validar o ambiente Airflow do projeto Retail Lakehouse",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    tags=["retail-lakehouse", "bootstrap", "validation"],
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    show_context = PythonOperator(
        task_id="show_project_context",
        python_callable=print_project_context,
    )

    show_next_steps = PythonOperator(
        task_id="show_next_steps",
        python_callable=print_next_steps,
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> show_context >> show_next_steps >> end