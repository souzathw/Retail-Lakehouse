from __future__ import annotations

import shutil
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, input_file_name, lit, trim
from pyspark.sql.types import IntegerType


def resolve_container_data_paths() -> dict[str, Path]:
    """
    Resolve os caminhos reais dentro do container Spark.
    """
    raw_dir = Path("/data/raw").resolve()
    bronze_dir = Path("/data/bronze").resolve()

    return {
        "raw_dir": raw_dir,
        "bronze_dir": bronze_dir,
    }


def find_latest_raw_partition(entity_dir: Path) -> Path:
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
    spark = (
        SparkSession.builder
        .appName("retail-lakehouse-bronze-order-products-prior")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_raw_order_products_prior(spark: SparkSession, raw_csv_path: Path) -> DataFrame:
    print("==================================================")
    print("Leitura da raw de order_products_prior")
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


def transform_order_products_prior_to_bronze(
    df: DataFrame,
    raw_ingestion_date: str,
) -> DataFrame:
    print("==================================================")
    print("Transformação Bronze de order_products_prior")
    print("==================================================")

    bronze_df = (
        df.select(
            trim(col("order_id")).alias("order_id"),
            trim(col("product_id")).alias("product_id"),
            trim(col("add_to_cart_order")).alias("add_to_cart_order"),
            trim(col("reordered")).alias("reordered"),
        )
        .filter(
            col("order_id").isNotNull()
            | col("product_id").isNotNull()
            | col("add_to_cart_order").isNotNull()
            | col("reordered").isNotNull()
        )
        .withColumn("order_id", col("order_id").cast(IntegerType()))
        .withColumn("product_id", col("product_id").cast(IntegerType()))
        .withColumn("add_to_cart_order", col("add_to_cart_order").cast(IntegerType()))
        .withColumn("reordered", col("reordered").cast(IntegerType()))
        .withColumn("source_file_name", input_file_name())
        .withColumn("raw_ingestion_date", lit(raw_ingestion_date))
        .withColumn("bronze_processed_at", current_timestamp())
    )

    print("Schema após transformação Bronze:")
    bronze_df.printSchema()

    return bronze_df


def write_bronze_order_products_prior(df: DataFrame, bronze_output_dir: Path) -> None:
    print("==================================================")
    print("Escrita da Bronze de order_products_prior")
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

    print("[OK] Bronze de order_products_prior escrita com sucesso em Parquet.")


def summarize_bronze_output(df: DataFrame, bronze_output_dir: Path) -> None:
    print("==================================================")
    print("Resumo da Bronze de order_products_prior")
    print("==================================================")
    print(f"Total de registros: {df.count()}")
    print(f"Diretório final: {bronze_output_dir}")

    print("Prévia dos dados:")
    df.show(20, truncate=False)


def run_bronze_order_products_prior() -> None:
    print("==================================================")
    print("Retail Lakehouse - Bronze de order_products_prior")
    print("==================================================")

    paths = resolve_container_data_paths()

    raw_entity_dir = paths["raw_dir"] / "instacart" / "order_products_prior"
    latest_raw_partition = find_latest_raw_partition(raw_entity_dir)
    raw_ingestion_date = extract_ingestion_date_from_partition(latest_raw_partition)
    raw_csv_path = latest_raw_partition / "order_products__prior.csv"

    bronze_output_dir = (
        paths["bronze_dir"]
        / "instacart"
        / "order_products_prior"
        / f"ingestion_date={raw_ingestion_date}"
    )

    spark = create_spark_session()

    try:
        raw_df = read_raw_order_products_prior(spark=spark, raw_csv_path=raw_csv_path)
        bronze_df = transform_order_products_prior_to_bronze(
            df=raw_df,
            raw_ingestion_date=raw_ingestion_date,
        )
        write_bronze_order_products_prior(
            df=bronze_df,
            bronze_output_dir=bronze_output_dir,
        )
        summarize_bronze_output(
            df=bronze_df,
            bronze_output_dir=bronze_output_dir,
        )
        print("[OK] Job Bronze de order_products_prior concluído com sucesso.")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_bronze_order_products_prior()