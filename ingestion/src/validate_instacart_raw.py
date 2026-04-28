from __future__ import annotations

import csv
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List


@dataclass(frozen=True)
class RawEntitySpec:
    entity_name: str
    expected_filename: str
    expected_columns: List[str]


RAW_ENTITY_SPECS: List[RawEntitySpec] = [
    RawEntitySpec(
        entity_name="orders",
        expected_filename="orders.csv",
        expected_columns=[
            "order_id",
            "user_id",
            "eval_set",
            "order_number",
            "order_dow",
            "order_hour_of_day",
            "days_since_prior_order",
        ],
    ),
    RawEntitySpec(
        entity_name="products",
        expected_filename="products.csv",
        expected_columns=[
            "product_id",
            "product_name",
            "aisle_id",
            "department_id",
        ],
    ),
    RawEntitySpec(
        entity_name="departments",
        expected_filename="departments.csv",
        expected_columns=[
            "department_id",
            "department",
        ],
    ),
    RawEntitySpec(
        entity_name="aisles",
        expected_filename="aisles.csv",
        expected_columns=[
            "aisle_id",
            "aisle",
        ],
    ),
    RawEntitySpec(
        entity_name="order_products_prior",
        expected_filename="order_products__prior.csv",
        expected_columns=[
            "order_id",
            "product_id",
            "add_to_cart_order",
            "reordered",
        ],
    ),
    RawEntitySpec(
        entity_name="order_products_train",
        expected_filename="order_products__train.csv",
        expected_columns=[
            "order_id",
            "product_id",
            "add_to_cart_order",
            "reordered",
        ],
    ),
]


def get_env_var(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise ValueError(f"Variável de ambiente obrigatória não encontrada: {name}")
    return value


def resolve_raw_base_dir() -> Path:
    raw_dir = Path(get_env_var("LOCAL_RAW_DIR", "./data/raw"))
    if not raw_dir.is_absolute():
        project_root = Path(__file__).resolve().parents[2]
        raw_dir = (project_root / raw_dir).resolve()
    return raw_dir


def find_latest_ingestion_partition(entity_dir: Path) -> Path:
    """
    Retorna a partição mais recente de ingestion_date para uma entidade.
    """
    if not entity_dir.exists():
        raise FileNotFoundError(f"Diretório da entidade não encontrado: {entity_dir}")

    partitions = [
        path for path in entity_dir.iterdir()
        if path.is_dir() and path.name.startswith("ingestion_date=")
    ]

    if not partitions:
        raise FileNotFoundError(
            f"Nenhuma partição ingestion_date encontrada em: {entity_dir}"
        )

    latest_partition = sorted(partitions, key=lambda p: p.name)[-1]
    return latest_partition


def count_csv_rows_and_extract_columns(csv_path: Path) -> Dict[str, object]:
    """
    Lê um CSV e retorna quantidade de linhas de dados, colunas e um indicador de vazio.
    """
    if not csv_path.exists():
        raise FileNotFoundError(f"Arquivo CSV não encontrado: {csv_path}")

    with csv_path.open(mode="r", encoding="utf-8", newline="") as file:
        reader = csv.reader(file)
        rows = list(reader)

    if not rows:
        return {
            "columns_found": [],
            "data_row_count": 0,
            "is_empty_file": True,
        }

    header = rows[0]
    data_row_count = max(len(rows) - 1, 0)

    return {
        "columns_found": header,
        "data_row_count": data_row_count,
        "is_empty_file": data_row_count == 0,
    }


def compare_columns(expected_columns: List[str], found_columns: List[str]) -> Dict[str, List[str]]:
    expected_set = set(expected_columns)
    found_set = set(found_columns)

    missing_columns = sorted(expected_set - found_set)
    unexpected_columns = sorted(found_set - expected_set)

    return {
        "missing_columns": missing_columns,
        "unexpected_columns": unexpected_columns,
    }


def validate_raw_entity(raw_base_dir: Path, spec: RawEntitySpec) -> Dict[str, object]:
    """
    Valida uma entidade específica da raw.
    """
    entity_dir = raw_base_dir / "instacart" / spec.entity_name
    latest_partition = find_latest_ingestion_partition(entity_dir)
    csv_path = latest_partition / spec.expected_filename

    csv_stats = count_csv_rows_and_extract_columns(csv_path)
    column_comparison = compare_columns(
        expected_columns=spec.expected_columns,
        found_columns=csv_stats["columns_found"],
    )

    result = {
        "entity_name": spec.entity_name,
        "partition_path": str(latest_partition),
        "csv_path": str(csv_path),
        "data_row_count": csv_stats["data_row_count"],
        "columns_found": csv_stats["columns_found"],
        "expected_columns": spec.expected_columns,
        "missing_columns": column_comparison["missing_columns"],
        "unexpected_columns": column_comparison["unexpected_columns"],
        "is_empty_file": csv_stats["is_empty_file"],
    }

    return result


def print_entity_validation(result: Dict[str, object]) -> None:
    """
    Exibe o resultado da validação de uma entidade.
    """
    print("--------------------------------------------------")
    print(f"Entidade: {result['entity_name']}")
    print(f"Partição usada: {result['partition_path']}")
    print(f"Arquivo CSV: {result['csv_path']}")
    print(f"Quantidade de linhas de dados: {result['data_row_count']}")
    print(f"Colunas encontradas: {result['columns_found']}")
    print(f"Colunas esperadas: {result['expected_columns']}")
    print(f"Colunas ausentes: {result['missing_columns']}")
    print(f"Colunas inesperadas: {result['unexpected_columns']}")
    print(f"Arquivo vazio: {result['is_empty_file']}")


def validate_instacart_raw() -> None:
    """
    Executa a validação completa da camada raw do Instacart.
    """
    print("==================================================")
    print("Retail Lakehouse - Validação da camada raw")
    print("==================================================")

    raw_base_dir = resolve_raw_base_dir()
    print(f"Diretório base raw: {raw_base_dir}")

    validation_results: List[Dict[str, object]] = []
    errors: List[str] = []

    for spec in RAW_ENTITY_SPECS:
        try:
            result = validate_raw_entity(raw_base_dir=raw_base_dir, spec=spec)
            validation_results.append(result)
            print_entity_validation(result)

            if result["is_empty_file"]:
                errors.append(f"Arquivo vazio para entidade: {spec.entity_name}")

            if result["missing_columns"]:
                errors.append(
                    f"Colunas ausentes em {spec.entity_name}: {result['missing_columns']}"
                )

        except Exception as exc:
            error_message = f"Erro ao validar entidade {spec.entity_name}: {exc}"
            print(error_message)
            errors.append(error_message)

    print("==================================================")
    print("Resumo final da validação raw")
    print("==================================================")
    print(f"Total de entidades validadas com leitura bem-sucedida: {len(validation_results)}")
    print(f"Total de erros encontrados: {len(errors)}")

    if errors:
        print("Erros encontrados:")
        for error in errors:
            print(f"- {error}")
        raise ValueError("Validação da camada raw falhou. Verifique os erros acima.")

    print("Validação da camada raw concluída com sucesso.")


if __name__ == "__main__":
    validate_instacart_raw()