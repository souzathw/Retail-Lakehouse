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


def resolve_container_data_paths() -> dict[str, Path]:
    """
    Resolve os caminhos reais dentro do container Spark.
    Pelo contexto do seu projeto, os dados estão montados em /data.
    """
    raw_dir = Path("/data/raw").resolve()
    bronze_dir = Path("/data/bronze").resolve()

    return {
        "raw_dir": raw_dir,
        "bronze_dir": bronze_dir,
    }


def find_latest_raw_partition(entity_dir: Path) -> Path:
    """
    Encontra a partição ingestion_date mais recente para a entidade.
    """
    if not entity_dir.exists():
        raise FileNotFoundError(f"Diretório raw não encontrado: {entity_dir}")

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


def extract_ingestion_date_from_partition(partition_path: Path) -> str:
    partition_name = partition_path.name
    prefix = "ingestion_date="
    if not partition_name.startswith(prefix):
        raise ValueError(f"Partição inválida: {partition_name}")
    return partition_name.replace(prefix, "")


def create_spark_session() -> SparkSession:
    spark_app_name = get_env_var("SPARK_APP_NAME", "retail-lakehouse-spark")

    spark = (
        SparkSession.builder
        .appName(f"{spark_app_name}-bronze-aisles")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_raw_aisles(spark: SparkSession, raw_csv_path: Path) -> DataFrame:
    print("==================================================")
    print("Leitura da raw de aisles")
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


def transform_aisles_to_bronze(df: DataFrame, raw_ingestion_date: str) -> DataFrame:
    print("==================================================")
    print("Transformação Bronze de aisles")
    print("==================================================")

    bronze_df = (
        df.select(
            trim(col("aisle_id")).alias("aisle_id"),
            trim(col("aisle")).alias("aisle"),
        )
        .filter(
            col("aisle_id").isNotNull() | col("aisle").isNotNull()
        )
        .withColumn("aisle_id", col("aisle_id").cast(IntegerType()))
        .withColumn("aisle", col("aisle").cast(StringType()))
        .withColumn("source_file_name", input_file_name())
        .withColumn("raw_ingestion_date", lit(raw_ingestion_date))
        .withColumn("bronze_processed_at", current_timestamp())
    )

    print("Schema após transformação Bronze:")
    bronze_df.printSchema()

    return bronze_df


def write_bronze_aisles(df: DataFrame, bronze_output_dir: Path) -> None:
    print("==================================================")
    print("Escrita da Bronze de aisles")
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

    print("[OK] Bronze de aisles escrita com sucesso em Parquet.")


def summarize_bronze_output(df: DataFrame, bronze_output_dir: Path) -> None:
    print("==================================================")
    print("Resumo da Bronze de aisles")
    print("==================================================")
    print(f"Total de registros: {df.count()}")
    print(f"Diretório final: {bronze_output_dir}")

    print("Prévia dos dados:")
    df.show(20, truncate=False)


def run_bronze_aisles() -> None:
    print("==================================================")
    print("Retail Lakehouse - Bronze de aisles")
    print("==================================================")

    paths = resolve_container_data_paths()

    raw_entity_dir = paths["raw_dir"] / "instacart" / "aisles"
    latest_raw_partition = find_latest_raw_partition(raw_entity_dir)
    raw_ingestion_date = extract_ingestion_date_from_partition(latest_raw_partition)
    raw_csv_path = latest_raw_partition / "aisles.csv"

    bronze_output_dir = (
        paths["bronze_dir"]
        / "instacart"
        / "aisles"
        / f"ingestion_date={raw_ingestion_date}"
    )

    spark = create_spark_session()

    try:
        raw_df = read_raw_aisles(spark=spark, raw_csv_path=raw_csv_path)
        bronze_df = transform_aisles_to_bronze(
            df=raw_df,
            raw_ingestion_date=raw_ingestion_date,
        )
        write_bronze_aisles(df=bronze_df, bronze_output_dir=bronze_output_dir)
        summarize_bronze_output(df=bronze_df, bronze_output_dir=bronze_output_dir)
        print("[OK] Job Bronze de aisles concluído com sucesso.")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_bronze_aisles()