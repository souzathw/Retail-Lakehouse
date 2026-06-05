from __future__ import annotations

import os
from pathlib import Path
from typing import List, Optional

import pyarrow.parquet as pq


EXPECTED_SILVER_COLUMNS: List[str] = [
    "product_id",
    "product_name",
    "aisle_id",
    "aisle",
    "department_id",
    "department",
    "bronze_raw_ingestion_date",
    "silver_processed_at",
]


def get_env_var(name: str, default: Optional[str] = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise ValueError(f"Variável de ambiente obrigatória não encontrada: {name}")
    return value


def resolve_project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def resolve_silver_base_dir() -> Path:
    silver_dir = Path(get_env_var("LOCAL_SILVER_DIR", "./data/silver"))
    if not silver_dir.is_absolute():
        project_root = resolve_project_root()
        silver_dir = (project_root / silver_dir).resolve()
    return silver_dir


def find_latest_silver_partition(entity_dir: Path) -> Path:
    if not entity_dir.exists():
        raise FileNotFoundError(f"Diretório Silver não encontrado: {entity_dir}")

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


def validate_silver_products_enriched() -> None:
    print("==================================================")
    print("Retail Lakehouse - Validação Silver products_enriched")
    print("==================================================")

    silver_base_dir = resolve_silver_base_dir()
    entity_dir = silver_base_dir / "instacart" / "products_enriched"
    latest_partition = find_latest_silver_partition(entity_dir)
    parquet_files = list_parquet_files(latest_partition)

    print(f"Diretório base Silver: {silver_base_dir}")
    print(f"Partição Silver usada: {latest_partition}")
    print(f"Arquivos Parquet encontrados: {[str(p.name) for p in parquet_files]}")

    table = pq.read_table(str(latest_partition))
    columns_found = table.column_names
    missing_columns = sorted(set(EXPECTED_SILVER_COLUMNS) - set(columns_found))
    unexpected_columns = sorted(set(columns_found) - set(EXPECTED_SILVER_COLUMNS))
    row_count = table.num_rows

    print("==================================================")
    print("Resultado da validação Silver products_enriched")
    print("==================================================")
    print(f"Total de linhas: {row_count}")
    print(f"Colunas encontradas: {columns_found}")
    print(f"Colunas esperadas: {EXPECTED_SILVER_COLUMNS}")
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
        errors.append("A Silver de products_enriched está vazia.")

    if missing_columns:
        errors.append(
            f"Colunas ausentes na Silver de products_enriched: {missing_columns}"
        )

    if errors:
        print("Erros encontrados:")
        for error in errors:
            print(f"- {error}")
        raise ValueError("Validação da Silver de products_enriched falhou.")

    print("[OK] Validação da Silver de products_enriched concluída com sucesso.")


if __name__ == "__main__":
    validate_silver_products_enriched()