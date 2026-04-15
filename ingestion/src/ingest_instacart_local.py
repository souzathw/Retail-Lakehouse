from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List


@dataclass(frozen=True)
class DatasetFile:
    entity_name: str
    source_filename: str


EXPECTED_FILES: List[DatasetFile] = [
    DatasetFile(entity_name="orders", source_filename="orders.csv"),
    DatasetFile(entity_name="products", source_filename="products.csv"),
    DatasetFile(entity_name="departments", source_filename="departments.csv"),
    DatasetFile(entity_name="aisles", source_filename="aisles.csv"),
    DatasetFile(entity_name="order_products_prior", source_filename="order_products__prior.csv"),
    DatasetFile(entity_name="order_products_train", source_filename="order_products__train.csv"),
]


def get_env_var(name: str, default: str | None = None) -> str:
    """
    Retorna uma variável de ambiente ou valor padrão.
    """
    value = os.getenv(name, default)
    if value is None:
        raise ValueError(f"Variável de ambiente obrigatória não encontrada: {name}")
    return value


def resolve_project_paths() -> Dict[str, Path]:
    """
    Resolve os caminhos principais do projeto a partir das variáveis de ambiente.
    """
    local_source_instacart_dir = Path(
        get_env_var("LOCAL_SOURCE_INSTACART_DIR", "./data/source/instacart")
    ).resolve()

    local_raw_dir = Path(
        get_env_var("LOCAL_RAW_DIR", "./data/raw")
    ).resolve()

    return {
        "source_instacart_dir": local_source_instacart_dir,
        "raw_dir": local_raw_dir,
    }


def validate_source_files(source_dir: Path) -> None:
    """
    Valida se todos os arquivos esperados do dataset estão presentes.
    """
    print("==================================================")
    print("Validação dos arquivos de origem do Instacart")
    print("==================================================")
    print(f"Diretório de origem: {source_dir}")

    if not source_dir.exists():
        raise FileNotFoundError(
            f"O diretório de origem não existe: {source_dir}"
        )

    missing_files: List[str] = []

    for dataset_file in EXPECTED_FILES:
        file_path = source_dir / dataset_file.source_filename
        if file_path.exists():
            print(f"[OK] Arquivo encontrado: {dataset_file.source_filename}")
        else:
            print(f"[ERRO] Arquivo ausente: {dataset_file.source_filename}")
            missing_files.append(dataset_file.source_filename)

    if missing_files:
        raise FileNotFoundError(
            "Arquivos obrigatórios ausentes no dataset de origem: "
            + ", ".join(missing_files)
        )

    print("Todos os arquivos obrigatórios foram encontrados.")


def build_raw_destination_path(raw_dir: Path, entity_name: str, ingestion_date: str) -> Path:
    """
    Monta o destino padrão da camada raw local.
    """
    return raw_dir / "instacart" / entity_name / f"ingestion_date={ingestion_date}"


def copy_source_files_to_raw(source_dir: Path, raw_dir: Path, ingestion_date: str) -> List[Path]:
    """
    Copia os arquivos do dataset da origem local para a camada raw local,
    organizando por entidade e data de ingestão.
    """
    print("==================================================")
    print("Cópia dos arquivos para a camada raw local")
    print("==================================================")

    copied_files: List[Path] = []

    for dataset_file in EXPECTED_FILES:
        source_file_path = source_dir / dataset_file.source_filename
        destination_dir = build_raw_destination_path(
            raw_dir=raw_dir,
            entity_name=dataset_file.entity_name,
            ingestion_date=ingestion_date,
        )
        destination_dir.mkdir(parents=True, exist_ok=True)

        destination_file_path = destination_dir / dataset_file.source_filename

        shutil.copy2(source_file_path, destination_file_path)

        copied_files.append(destination_file_path)

        print(f"[COPIADO] {source_file_path} -> {destination_file_path}")

    print("Cópia concluída com sucesso.")
    return copied_files


def summarize_raw_ingestion(copied_files: List[Path], ingestion_date: str) -> None:
    """
    Exibe um resumo da ingestão executada.
    """
    print("==================================================")
    print("Resumo da ingestão local para raw")
    print("==================================================")
    print(f"Data de ingestão: {ingestion_date}")
    print(f"Total de arquivos copiados: {len(copied_files)}")

    for file_path in copied_files:
        file_size_bytes = file_path.stat().st_size
        print(f"- {file_path} | tamanho={file_size_bytes} bytes")


def run_local_raw_ingestion() -> None:
    """
    Executa o fluxo completo de ingestão local do dataset Instacart para a raw.
    """
    print("==================================================")
    print("Retail Lakehouse - Ingestão local para camada raw")
    print("==================================================")

    paths = resolve_project_paths()
    source_dir = paths["source_instacart_dir"]
    raw_dir = paths["raw_dir"]

    ingestion_date = datetime.now().strftime("%Y-%m-%d")

    validate_source_files(source_dir=source_dir)
    copied_files = copy_source_files_to_raw(
        source_dir=source_dir,
        raw_dir=raw_dir,
        ingestion_date=ingestion_date,
    )
    summarize_raw_ingestion(
        copied_files=copied_files,
        ingestion_date=ingestion_date,
    )

    print("Ingestão local concluída com sucesso.")


if __name__ == "__main__":
    run_local_raw_ingestion()