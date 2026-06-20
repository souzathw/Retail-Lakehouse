from __future__ import annotations

import shutil
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, countDistinct, current_timestamp, round as spark_round


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
        .appName("retail-lakehouse-gold-orders-by-period")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_silver_orders_clean(spark: SparkSession, path: Path) -> DataFrame:
    print("==================================================")
    print("Leitura da Silver de orders_clean")
    print("==================================================")
    print(f"Diretório de entrada: {path}")

    df = spark.read.parquet(str(path))

    print("Schema da Silver de orders_clean:")
    df.printSchema()

    return df


def build_gold_orders_by_period(orders_clean_df: DataFrame) -> DataFrame:
    print("==================================================")
    print("Transformação Gold de orders_by_period")
    print("==================================================")

    grouped_df = (
        orders_clean_df.groupBy(
            col("order_dow"),
            col("order_hour_of_day"),
        )
        .agg(
            countDistinct("order_id").alias("total_orders"),
            countDistinct("user_id").alias("distinct_users"),
        )
        .withColumn(
            "avg_orders_per_user",
            spark_round(col("total_orders") / col("distinct_users"), 4),
        )
        .withColumn("gold_processed_at", current_timestamp())
        .orderBy(col("order_dow"), col("order_hour_of_day"))
    )

    print("Schema após agregação Gold:")
    grouped_df.printSchema()

    return grouped_df


def write_gold_orders_by_period(df: DataFrame, output_dir: Path) -> None:
    print("==================================================")
    print("Escrita da Gold de orders_by_period")
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

    print("[OK] Gold de orders_by_period escrita com sucesso em Parquet.")


def summarize_gold_output(df: DataFrame, output_dir: Path) -> None:
    print("==================================================")
    print("Resumo da Gold de orders_by_period")
    print("==================================================")
    print(f"Total de linhas agregadas: {df.count()}")
    print(f"Diretório final: {output_dir}")

    print("Prévia dos dados:")
    df.show(30, truncate=False)


def run_gold_orders_by_period() -> None:
    print("==================================================")
    print("Retail Lakehouse - Gold de orders_by_period")
    print("==================================================")

    paths = resolve_container_data_paths()

    orders_clean_dir = paths["silver_dir"] / "instacart" / "orders_clean"
    latest_orders_clean_partition = find_latest_partition(orders_clean_dir)

    gold_ingestion_date = extract_ingestion_date_from_partition(latest_orders_clean_partition)

    output_dir = (
        paths["gold_dir"]
        / "instacart"
        / "orders_by_period"
        / f"ingestion_date={gold_ingestion_date}"
    )

    spark = create_spark_session()

    try:
        orders_clean_df = read_silver_orders_clean(
            spark=spark,
            path=latest_orders_clean_partition,
        )
        gold_df = build_gold_orders_by_period(orders_clean_df=orders_clean_df)
        write_gold_orders_by_period(df=gold_df, output_dir=output_dir)
        summarize_gold_output(df=gold_df, output_dir=output_dir)

        print("[OK] Job Gold de orders_by_period concluído com sucesso.")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_gold_orders_by_period()