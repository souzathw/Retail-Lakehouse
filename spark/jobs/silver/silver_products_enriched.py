from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp


def get_env_var(name: str, default: Optional[str] = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise ValueError(f"Variável de ambiente obrigatória não encontrada: {name}")
    return value


def resolve_container_data_paths() -> dict[str, Path]:
    bronze_dir = Path("/data/bronze").resolve()
    silver_dir = Path("/data/silver").resolve()

    return {
        "bronze_dir": bronze_dir,
        "silver_dir": silver_dir,
    }


def find_latest_partition(entity_dir: Path) -> Path:
    if not entity_dir.exists():
        raise FileNotFoundError(f"Diretório não encontrado: {entity_dir}")

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
        .appName(f"{spark_app_name}-silver-products-enriched")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_bronze_dataset(spark: SparkSession, path: Path, dataset_name: str) -> DataFrame:
    print("==================================================")
    print(f"Leitura da Bronze de {dataset_name}")
    print("==================================================")
    print(f"Diretório de entrada: {path}")

    if not path.exists():
        raise FileNotFoundError(f"Diretório Bronze não encontrado para {dataset_name}: {path}")

    df = spark.read.parquet(str(path))

    print(f"Schema da Bronze de {dataset_name}:")
    df.printSchema()

    return df


def build_silver_products_enriched(
    products_df: DataFrame,
    aisles_df: DataFrame,
    departments_df: DataFrame,
) -> DataFrame:
    print("==================================================")
    print("Transformação Silver de products_enriched")
    print("==================================================")

    silver_df = (
        products_df.alias("p")
        .join(
            aisles_df.alias("a"),
            on=col("p.aisle_id") == col("a.aisle_id"),
            how="left",
        )
        .join(
            departments_df.alias("d"),
            on=col("p.department_id") == col("d.department_id"),
            how="left",
        )
        .select(
            col("p.product_id").alias("product_id"),
            col("p.product_name").alias("product_name"),
            col("p.aisle_id").alias("aisle_id"),
            col("a.aisle").alias("aisle"),
            col("p.department_id").alias("department_id"),
            col("d.department").alias("department"),
            col("p.raw_ingestion_date").alias("bronze_raw_ingestion_date"),
            current_timestamp().alias("silver_processed_at"),
        )
    )

    print("Schema após enriquecimento Silver:")
    silver_df.printSchema()

    return silver_df


def write_silver_products_enriched(df: DataFrame, output_dir: Path) -> None:
    print("==================================================")
    print("Escrita da Silver de products_enriched")
    print("==================================================")
    print(f"Diretório de saída: {output_dir}")

    if output_dir.exists():
        shutil.rmtree(output_dir)

    output_dir.mkdir(parents=True, exist_ok=True)

    (
        df.write
        .mode("overwrite")
        .parquet(str(output_dir))
    )

    print("[OK] Silver de products_enriched escrita com sucesso em Parquet.")


def summarize_silver_output(df: DataFrame, output_dir: Path) -> None:
    print("==================================================")
    print("Resumo da Silver de products_enriched")
    print("==================================================")
    print(f"Total de registros: {df.count()}")
    print(f"Diretório final: {output_dir}")

    print("Prévia dos dados:")
    df.show(20, truncate=False)


def run_silver_products_enriched() -> None:
    print("==================================================")
    print("Retail Lakehouse - Silver de products_enriched")
    print("==================================================")

    paths = resolve_container_data_paths()

    products_dir = paths["bronze_dir"] / "instacart" / "products"
    aisles_dir = paths["bronze_dir"] / "instacart" / "aisles"
    departments_dir = paths["bronze_dir"] / "instacart" / "departments"

    latest_products_partition = find_latest_partition(products_dir)
    latest_aisles_partition = find_latest_partition(aisles_dir)
    latest_departments_partition = find_latest_partition(departments_dir)

    silver_ingestion_date = extract_ingestion_date_from_partition(latest_products_partition)

    output_dir = (
        paths["silver_dir"]
        / "instacart"
        / "products_enriched"
        / f"ingestion_date={silver_ingestion_date}"
    )

    spark = create_spark_session()

    try:
        products_df = read_bronze_dataset(
            spark=spark,
            path=latest_products_partition,
            dataset_name="products",
        )
        aisles_df = read_bronze_dataset(
            spark=spark,
            path=latest_aisles_partition,
            dataset_name="aisles",
        )
        departments_df = read_bronze_dataset(
            spark=spark,
            path=latest_departments_partition,
            dataset_name="departments",
        )

        silver_df = build_silver_products_enriched(
            products_df=products_df,
            aisles_df=aisles_df,
            departments_df=departments_df,
        )
        write_silver_products_enriched(df=silver_df, output_dir=output_dir)
        summarize_silver_output(df=silver_df, output_dir=output_dir)

        print("[OK] Job Silver de products_enriched concluído com sucesso.")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_silver_products_enriched()