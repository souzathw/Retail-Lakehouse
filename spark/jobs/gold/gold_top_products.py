from __future__ import annotations

import shutil
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    current_timestamp,
    round as spark_round,
    sum as spark_sum,
)


def resolve_container_data_paths() -> dict[str, Path]:
    """
    Resolve os caminhos reais dentro do container Spark.
    """
    silver_dir = Path("/data/silver").resolve()
    gold_dir = Path("/data/gold").resolve()

    return {
        "silver_dir": silver_dir,
        "gold_dir": gold_dir,
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
    spark = (
        SparkSession.builder
        .appName("retail-lakehouse-gold-top-products")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_silver_order_items_prior_enriched(spark: SparkSession, path: Path) -> DataFrame:
    print("==================================================")
    print("Leitura da Silver de order_items_prior_enriched")
    print("==================================================")
    print(f"Diretório de entrada: {path}")

    df = spark.read.parquet(str(path))

    print("Schema da Silver de order_items_prior_enriched:")
    df.printSchema()

    return df


def build_gold_top_products(order_items_prior_enriched_df: DataFrame) -> DataFrame:
    print("==================================================")
    print("Transformação Gold de top_products")
    print("==================================================")

    grouped_df = (
        order_items_prior_enriched_df.groupBy(
            col("product_id"),
            col("product_name"),
            col("aisle_id"),
            col("aisle"),
            col("department_id"),
            col("department"),
        )
        .agg(
            count("*").alias("total_order_rows"),
            countDistinct("order_id").alias("distinct_orders"),
            countDistinct("user_id").alias("distinct_users"),
            spark_sum("reordered").alias("total_reorders"),
        )
        .withColumn(
            "reorder_rate",
            spark_round(col("total_reorders") / col("total_order_rows"), 4),
        )
        .withColumn("gold_processed_at", current_timestamp())
        .orderBy(col("total_order_rows").desc(), col("distinct_orders").desc())
    )

    print("Schema após agregação Gold:")
    grouped_df.printSchema()

    return grouped_df


def write_gold_top_products(df: DataFrame, output_dir: Path) -> None:
    print("==================================================")
    print("Escrita da Gold de top_products")
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

    print("[OK] Gold de top_products escrita com sucesso em Parquet.")


def summarize_gold_output(df: DataFrame, output_dir: Path) -> None:
    print("==================================================")
    print("Resumo da Gold de top_products")
    print("==================================================")
    print(f"Total de linhas agregadas: {df.count()}")
    print(f"Diretório final: {output_dir}")

    print("Prévia dos dados:")
    df.show(30, truncate=False)


def run_gold_top_products() -> None:
    print("==================================================")
    print("Retail Lakehouse - Gold de top_products")
    print("==================================================")

    paths = resolve_container_data_paths()

    order_items_prior_enriched_dir = (
        paths["silver_dir"] / "instacart" / "order_items_prior_enriched"
    )
    latest_input_partition = find_latest_partition(order_items_prior_enriched_dir)

    gold_ingestion_date = extract_ingestion_date_from_partition(latest_input_partition)

    output_dir = (
        paths["gold_dir"]
        / "instacart"
        / "top_products"
        / f"ingestion_date={gold_ingestion_date}"
    )

    spark = create_spark_session()

    try:
        input_df = read_silver_order_items_prior_enriched(
            spark=spark,
            path=latest_input_partition,
        )
        gold_df = build_gold_top_products(order_items_prior_enriched_df=input_df)
        write_gold_top_products(df=gold_df, output_dir=output_dir)
        summarize_gold_output(df=gold_df, output_dir=output_dir)

        print("[OK] Job Gold de top_products concluído com sucesso.")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_gold_top_products()