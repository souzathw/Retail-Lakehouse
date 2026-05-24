from __future__ import annotations

import os
from pathlib import Path
from typing import List, Optional

import pyarrow.parquet as pq


EXPECTED_BRONZE_COLUMNS: List[str] = [
    "order_id",
    "user_id",
    "eval_set",
    "order_number",
    "order_dow",
    "order_hour_of_day",
    "days_since_prior_order",
    "source_file_name",
    "raw_ingestion_date",
    "bronze_processed_at",
]


def get_env_var(name: str, default: Optional[str] = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise ValueError(f"Variável de ambiente obrigatória não encontrada: {name}")
    return value


def resolve_project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def resolve_bronze_base_dir() -> Path:
    bronze_dir = Path(get_env_var("LOCAL_BRONZE_DIR", "./data/bronze"))
    if not bronze_dir.is_absolute():
        project_root = resolve_project_root()
        bronze_dir = (project_root / bronze_dir).resolve()
    return bronze_dir


def find_latest_bronze_partition(entity_dir: Path) -> Path:
    if not entity_dir.exists():
        raise FileNotFoundError(f"Diretório Bronze não encontrado: {entity_dir}")

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


def list_parquet_files(partition_dir: Path) -> List[Path]:
    parquet_files = sorted(partition_dir.glob("*.parquet"))

    if not parquet_files:
        raise FileNotFoundError(
            f"Nenhum arquivo Parquet encontrado em: {partition_dir}"
        )

    return parquet_files


def validate_bronze_orders() -> None:
    print("==================================================")
    print("Retail Lakehouse - Validação Bronze orders")
    print("==================================================")

    bronze_base_dir = resolve_bronze_base_dir()
    entity_dir = bronze_base_dir / "instacart" / "orders"
    latest_partition = find_latest_bronze_partition(entity_dir)
    parquet_files = list_parquet_files(latest_partition)

    print(f"Diretório base Bronze: {bronze_base_dir}")
    print(f"Partição Bronze usada: {latest_partition}")
    print(f"Arquivos Parquet encontrados: {[str(p.name) for p in parquet_files]}")

    table = pq.read_table(str(latest_partition))
    columns_found = table.column_names
    missing_columns = sorted(set(EXPECTED_BRONZE_COLUMNS) - set(columns_found))
    unexpected_columns = sorted(set(columns_found) - set(EXPECTED_BRONZE_COLUMNS))
    row_count = table.num_rows

    print("==================================================")
    print("Resultado da validação Bronze orders")
    print("==================================================")
    print(f"Total de linhas: {row_count}")
    print(f"Colunas encontradas: {columns_found}")
    print(f"Colunas esperadas: {EXPECTED_BRONZE_COLUMNS}")
    print(f"Colunas ausentes: {missing_columns}")
    print(f"Colunas inesperadas: {unexpected_columns}")
    print("Schema Arrow:")
    print(table.schema)

    preview_rows = min(20, row_count)
    if preview_rows > 0:
        print("Prévia dos dados:")
        preview_df = table.slice(0, preview_rows).to_pandas()
        print(preview_df.to_string(index=False))

    errors: List[str] = []

    if row_count == 0:
        errors.append("A Bronze de orders está vazia.")

    if missing_columns:
        errors.append(f"Colunas ausentes na Bronze de orders: {missing_columns}")

    if errors:
        print("Erros encontrados:")
        for error in errors:
            print(f"- {error}")
        raise ValueError("Validação da Bronze de orders falhou.")

    print("[OK] Validação da Bronze de orders concluída com sucesso.")


if __name__ == "__main__":
    validate_bronze_orders()