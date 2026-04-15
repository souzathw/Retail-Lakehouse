from __future__ import annotations

import os
import shutil
import zipfile
from pathlib import Path
from typing import Final

from kaggle.api.kaggle_api_extended import KaggleApi


EXPECTED_OUTPUT_FILES: Final[list[str]] = [
    "orders.csv",
    "products.csv",
    "departments.csv",
    "aisles.csv",
    "order_products__prior.csv",
    "order_products__train.csv",
]


def get_env_var(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise ValueError(f"Variável de ambiente obrigatória não encontrada: {name}")
    return value


def resolve_paths() -> dict[str, Path]:
    project_root = Path(__file__).resolve().parents[2]

    dataset_download_dir = (project_root / "data" / "downloads").resolve()
    source_instacart_dir = Path(
        get_env_var("LOCAL_SOURCE_INSTACART_DIR", "./data/source/instacart")
    )
    if not source_instacart_dir.is_absolute():
        source_instacart_dir = (project_root / source_instacart_dir).resolve()

    kaggle_json_path = (project_root / "secrets" / "kaggle.json").resolve()

    return {
        "project_root": project_root,
        "download_dir": dataset_download_dir,
        "source_instacart_dir": source_instacart_dir,
        "kaggle_json_path": kaggle_json_path,
    }


def ensure_kaggle_credentials(kaggle_json_path: Path) -> None:
    print("==================================================")
    print("Validação de credenciais Kaggle")
    print("==================================================")

    if not kaggle_json_path.exists():
        raise FileNotFoundError(
            f"Arquivo de credenciais do Kaggle não encontrado em: {kaggle_json_path}"
        )

    kaggle_config_dir = kaggle_json_path.parent
    os.environ["KAGGLE_CONFIG_DIR"] = str(kaggle_config_dir)

    print(f"[OK] kaggle.json encontrado em: {kaggle_json_path}")
    print(f"[OK] KAGGLE_CONFIG_DIR definido como: {kaggle_config_dir}")


def authenticate_kaggle() -> KaggleApi:
    print("==================================================")
    print("Autenticação na API do Kaggle")
    print("==================================================")

    api = KaggleApi()
    api.authenticate()

    print("[OK] Autenticação no Kaggle realizada com sucesso.")
    return api


def clean_directory(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def download_dataset_zip(api: KaggleApi, dataset_slug: str, download_dir: Path) -> Path:
    print("==================================================")
    print("Download do dataset do Kaggle")
    print("==================================================")

    download_dir.mkdir(parents=True, exist_ok=True)

    api.dataset_download_files(
        dataset=dataset_slug,
        path=str(download_dir),
        unzip=False,
        quiet=False,
    )

    expected_zip_name = f"{dataset_slug.split('/')[-1]}.zip"
    zip_path = download_dir / expected_zip_name

    if not zip_path.exists():
        raise FileNotFoundError(
            f"Arquivo zip esperado não encontrado após download: {zip_path}"
        )

    print(f"[OK] Dataset baixado com sucesso: {zip_path}")
    return zip_path


def extract_dataset_zip(zip_path: Path, destination_dir: Path) -> None:
    print("==================================================")
    print("Extração do dataset")
    print("==================================================")

    clean_directory(destination_dir)

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(destination_dir)

    print(f"[OK] Dataset extraído em: {destination_dir}")


def validate_extracted_files(destination_dir: Path) -> None:
    print("==================================================")
    print("Validação dos arquivos extraídos")
    print("==================================================")

    missing_files: list[str] = []

    for filename in EXPECTED_OUTPUT_FILES:
        filepath = destination_dir / filename
        if filepath.exists():
            print(f"[OK] Arquivo encontrado: {filename}")
        else:
            print(f"[ERRO] Arquivo ausente: {filename}")
            missing_files.append(filename)

    if missing_files:
        raise FileNotFoundError(
            "Arquivos esperados não encontrados após extração: "
            + ", ".join(missing_files)
        )

    print("[OK] Todos os arquivos esperados estão disponíveis.")


def summarize_source_preparation(source_instacart_dir: Path) -> None:
    print("==================================================")
    print("Resumo da preparação da origem")
    print("==================================================")

    files = sorted(source_instacart_dir.glob("*.csv"))
    print(f"Diretório final: {source_instacart_dir}")
    print(f"Total de arquivos CSV: {len(files)}")

    for file in files:
        print(f"- {file.name} | tamanho={file.stat().st_size} bytes")


def prepare_instacart_source_from_kaggle() -> None:

    print("==================================================")
    print("Retail Lakehouse - Preparação da origem via Kaggle")
    print("==================================================")

    dataset_slug = get_env_var("KAGGLE_DATASET", "psparks/instacart-market-basket-analysis")
    paths = resolve_paths()

    ensure_kaggle_credentials(paths["kaggle_json_path"])
    api = authenticate_kaggle()
    zip_path = download_dataset_zip(
        api=api,
        dataset_slug=dataset_slug,
        download_dir=paths["download_dir"],
    )
    extract_dataset_zip(zip_path=zip_path, destination_dir=paths["source_instacart_dir"])
    validate_extracted_files(destination_dir=paths["source_instacart_dir"])
    summarize_source_preparation(source_instacart_dir=paths["source_instacart_dir"])

    print("[OK] Origem do Instacart preparada com sucesso.")


if __name__ == "__main__":
    prepare_instacart_source_from_kaggle()