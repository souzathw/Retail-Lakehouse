from __future__ import annotations

from pathlib import Path
from typing import List

import pyarrow.parquet as pq


EXPECTED_GOLD_COLUMNS: List[str] = [
    "product_id",
    "product_name",
    "aisle_id",
    "aisle",
    "department_id",
    "department",
    "total_order_rows",
    "distinct_orders",
    "distinct_users",
    "total_reorders",
    "reorder_rate",
    "gold_processed_at",
]


def resolve_gold_base_dir() -> Path:
    """
    No container do Airflow, ./data está montado em /opt/airflow/data.
    """
    return Path("/opt/airflow/data/gold").resolve()


def find_latest_gold_partition(entity_dir: Path) -> Path:
    if not entity_dir.exists():
        raise FileNotFoundError(f"Diretório Gold não encontrado: {entity_dir}")

    partitions = [
        path
        for path in entity_dir.iterdir()
        if path.is_dir() and path.name.startswith("ingestion_date=")
    ]

    if not partitions:
        raise FileNotFoundError(
            f"Nenhuma partição ingestion_date encontrada em: {entity_dir}"
        )

    return sorted(partitions, key=lambda p: p.name)[-1]


def find_parquet_files(partition_dir: Path) -> List[Path]:
    parquet_files = sorted(partition_dir.glob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(
            f"Nenhum arquivo .parquet encontrado em: {partition_dir}"
        )
    return parquet_files


def validate_gold_top_products() -> None:
    print("==================================================")
    print("Retail Lakehouse - Validação Gold top_products")
    print("==================================================")

    gold_base_dir = resolve_gold_base_dir()
    entity_dir = gold_base_dir / "instacart" / "top_products"
    latest_partition = find_latest_gold_partition(entity_dir)
    parquet_files = find_parquet_files(latest_partition)

    print(f"Diretório base Gold: {gold_base_dir}")
    print(f"Partição Gold usada: {latest_partition}")
    print(f"Total de arquivos parquet: {len(parquet_files)}")

    first_parquet_file = parquet_files[0]
    parquet_file = pq.ParquetFile(first_parquet_file)
    schema_names = parquet_file.schema_arrow.names

    table = pq.read_table(first_parquet_file)
    row_count = table.num_rows

    missing_columns = sorted(set(EXPECTED_GOLD_COLUMNS) - set(schema_names))
    unexpected_columns = sorted(set(schema_names) - set(EXPECTED_GOLD_COLUMNS))

    print("==================================================")
    print("Resultado da validação Gold top_products")
    print("==================================================")
    print(f"Arquivo analisado: {first_parquet_file}")
    print(f"Total de linhas no arquivo analisado: {row_count}")
    print(f"Colunas encontradas: {schema_names}")
    print(f"Colunas esperadas: {EXPECTED_GOLD_COLUMNS}")
    print(f"Colunas ausentes: {missing_columns}")
    print(f"Colunas inesperadas: {unexpected_columns}")

    errors: List[str] = []

    if row_count == 0:
        errors.append("A Gold de top_products está vazia no arquivo analisado.")

    if missing_columns:
        errors.append(f"Colunas ausentes na Gold de top_products: {missing_columns}")

    if errors:
        print("Erros encontrados:")
        for error in errors:
            print(f"- {error}")
        raise ValueError("Validação da Gold de top_products falhou.")

    print("[OK] Validação da Gold de top_products concluída com sucesso.")