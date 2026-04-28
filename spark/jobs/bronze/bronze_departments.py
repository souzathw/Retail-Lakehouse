from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, input_file_name, lit, trim
from pyspark.sql.types import IntegerType, StringType


def get_env_var(name: str, default: Optional[str] = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise ValueError(f"Variável de ambiente obrigatória não encontrada: {name}")
    return value


def resolve_container_data_root() -> Path:
    container_data_root = Path("/data")
    if container_data_root.exists():
        return container_data_root
    return Path(__file__).resolve().parents[3] / "data"


def resolve_data_paths() -> dict[str, Path]:
    data_root = resolve_container_data_root()

    local_raw_dir = Path(get_env_var("LOCAL_RAW_DIR", "./data/raw"))
    local_bronze_dir = Path(get_env_var("LOCAL_BRONZE_DIR", "./data/bronze"))

    if not local_raw_dir.is_absolute():
        local_raw_dir = data_root / local_raw_dir.name
        if local_raw_dir.name != "raw":
            local_raw_dir = data_root / "raw"

    if not local_bronze_dir.is_absolute():
        local_bronze_dir = data_root / local_bronze_dir.name
        if local_bronze_dir.name != "bronze":
            local_bronze_dir = data_root / "bronze"

    return {
        "raw_dir": local_raw_dir.resolve(),
        "bronze_dir": local_bronze_dir.resolve(),
    }


def find_latest_raw_partition(entity_dir: Path) -> Path:
    if not entity_dir.exists():
        raise FileNotFoundError(f"Diretório da entidade raw não encontrado: {entity_dir}")

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


def extract_ingestion_date_from_partition(partition_path: Path) -> str:
    partition_name = partition_path.name
    prefix = "ingestion_date="
    if not partition_name.startswith(prefix):
        raise ValueError(f"Partição inválida: {partition_name}")
    return partition_name.replace(prefix, "")


def create_spark_session() -> SparkSession:
    spark_app_name = get_env_var("SPARK_APP_NAME", "retail-lakehouse-spark")
    spark_master = get_env_var("SPARK_MASTER", "local[*]")

    spark = (
        SparkSession.builder
        .appName(f"{spark_app_name}-bronze-departments")
        .master(spark_master)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_raw_departments(spark: SparkSession, raw_csv_path: Path) -> DataFrame:
    print("==================================================")
    print("Leitura da raw de departments")
    print("==================================================")
    print(f"Arquivo de entrada: {raw_csv_path}")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(str(raw_csv_path))
    )

    print("Schema lido da raw:")
    df.printSchema()

    return df


def transform_departments_to_bronze(df: DataFrame, raw_ingestion_date: str) -> DataFrame:
    print("==================================================")
    print("Transformação Bronze de departments")
    print("==================================================")

    bronze_df = (
        df.select(
            trim(col("department_id")).alias("department_id"),
            trim(col("department")).alias("department"),
        )
        .filter(
            col("department_id").isNotNull() | col("department").isNotNull()
        )
        .withColumn("department_id", col("department_id").cast(IntegerType()))
        .withColumn("department", col("department").cast(StringType()))
        .withColumn("source_file_name", input_file_name())
        .withColumn("raw_ingestion_date", lit(raw_ingestion_date))
        .withColumn("bronze_processed_at", current_timestamp())
    )

    print("Schema após transformação Bronze:")
    bronze_df.printSchema()

    return bronze_df


def write_bronze_departments(df: DataFrame, bronze_output_dir: Path) -> None:
    print("==================================================")
    print("Escrita da Bronze de departments")
    print("==================================================")
    print(f"Diretório de saída: {bronze_output_dir}")

    if bronze_output_dir.exists():
        shutil.rmtree(bronze_output_dir)

    bronze_output_dir.mkdir(parents=True, exist_ok=True)

    (
        df.write
        .mode("overwrite")
        .parquet(str(bronze_output_dir))
    )

    print("[OK] Bronze escrita com sucesso em Parquet.")


def summarize_bronze_output(df: DataFrame, bronze_output_dir: Path) -> None:
    print("==================================================")
    print("Resumo da Bronze de departments")
    print("==================================================")
    print(f"Total de registros: {df.count()}")
    print(f"Diretório final: {bronze_output_dir}")

    print("Prévia dos dados:")
    df.show(20, truncate=False)


def run_bronze_departments() -> None:
    print("==================================================")
    print("Retail Lakehouse - Bronze de departments")
    print("==================================================")

    paths = resolve_data_paths()

    print(f"RAW DIR resolvido: {paths['raw_dir']}")
    print(f"BRONZE DIR resolvido: {paths['bronze_dir']}")

    raw_entity_dir = paths["raw_dir"] / "instacart" / "departments"
    latest_raw_partition = find_latest_raw_partition(raw_entity_dir)
    raw_ingestion_date = extract_ingestion_date_from_partition(latest_raw_partition)
    raw_csv_path = latest_raw_partition / "departments.csv"

    bronze_output_dir = (
        paths["bronze_dir"]
        / "instacart"
        / "departments"
        / f"ingestion_date={raw_ingestion_date}"
    )

    spark = create_spark_session()

    try:
        raw_df = read_raw_departments(spark=spark, raw_csv_path=raw_csv_path)
        bronze_df = transform_departments_to_bronze(
            df=raw_df,
            raw_ingestion_date=raw_ingestion_date,
        )
        write_bronze_departments(df=bronze_df, bronze_output_dir=bronze_output_dir)
        summarize_bronze_output(df=bronze_df, bronze_output_dir=bronze_output_dir)
        print("[OK] Job Bronze de departments concluído com sucesso.")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_bronze_departments()