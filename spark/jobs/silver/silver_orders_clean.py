from __future__ import annotations

import shutil
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lower,
    trim,
    when,
)
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType


def resolve_container_data_paths() -> dict[str, Path]:
    """
    Resolve os caminhos reais dentro do container Spark.
    """
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
    spark = (
        SparkSession.builder
        .appName("retail-lakehouse-silver-orders-clean")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_bronze_orders(spark: SparkSession, path: Path) -> DataFrame:
    print("==================================================")
    print("Leitura da Bronze de orders")
    print("==================================================")
    print(f"Diretório de entrada: {path}")

    df = spark.read.parquet(str(path))

    print("Schema da Bronze de orders:")
    df.printSchema()

    return df


def build_silver_orders_clean(orders_df: DataFrame) -> DataFrame:
    print("==================================================")
    print("Transformação Silver de orders_clean")
    print("==================================================")

    silver_df = (
        orders_df.select(
            col("order_id"),
            col("user_id"),
            lower(trim(col("eval_set"))).alias("eval_set"),
            col("order_number"),
            col("order_dow"),
            col("order_hour_of_day"),
            col("days_since_prior_order"),
            col("raw_ingestion_date").alias("bronze_raw_ingestion_date"),
        )
        .withColumn("order_id", col("order_id").cast(IntegerType()))
        .withColumn("user_id", col("user_id").cast(IntegerType()))
        .withColumn("eval_set", col("eval_set").cast(StringType()))
        .withColumn("order_number", col("order_number").cast(IntegerType()))
        .withColumn("order_dow", col("order_dow").cast(IntegerType()))
        .withColumn("order_hour_of_day", col("order_hour_of_day").cast(IntegerType()))
        .withColumn("days_since_prior_order", col("days_since_prior_order").cast(DoubleType()))
        .withColumn(
            "is_first_order",
            when(col("order_number") == 1, True).otherwise(False).cast(BooleanType()),
        )
        .withColumn(
            "days_since_prior_order",
            when(col("order_number") == 1, 0.0).otherwise(col("days_since_prior_order")),
        )
        .filter(col("order_id").isNotNull())
        .filter(col("user_id").isNotNull())
        .filter(col("order_number").isNotNull())
        .filter(col("order_dow").isNotNull())
        .filter(col("order_hour_of_day").isNotNull())
        .filter(col("eval_set").isNotNull())
        .filter(col("order_dow").between(0, 6))
        .filter(col("order_hour_of_day").between(0, 23))
        .dropDuplicates(["order_id"])
        .withColumn("silver_processed_at", current_timestamp())
    )

    print("Schema após transformação Silver:")
    silver_df.printSchema()

    return silver_df


def write_silver_orders_clean(df: DataFrame, output_dir: Path) -> None:
    print("==================================================")
    print("Escrita da Silver de orders_clean")
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

    print("[OK] Silver de orders_clean escrita com sucesso em Parquet.")


def summarize_silver_output(df: DataFrame, output_dir: Path) -> None:
    print("==================================================")
    print("Resumo da Silver de orders_clean")
    print("==================================================")
    print(f"Total de registros: {df.count()}")
    print(f"Diretório final: {output_dir}")

    print("Prévia dos dados:")
    df.show(20, truncate=False)


def run_silver_orders_clean() -> None:
    print("==================================================")
    print("Retail Lakehouse - Silver de orders_clean")
    print("==================================================")

    paths = resolve_container_data_paths()

    orders_dir = paths["bronze_dir"] / "instacart" / "orders"
    latest_orders_partition = find_latest_partition(orders_dir)

    silver_ingestion_date = extract_ingestion_date_from_partition(latest_orders_partition)

    output_dir = (
        paths["silver_dir"]
        / "instacart"
        / "orders_clean"
        / f"ingestion_date={silver_ingestion_date}"
    )

    spark = create_spark_session()

    try:
        orders_df = read_bronze_orders(spark=spark, path=latest_orders_partition)
        silver_df = build_silver_orders_clean(orders_df=orders_df)
        write_silver_orders_clean(df=silver_df, output_dir=output_dir)
        summarize_silver_output(df=silver_df, output_dir=output_dir)

        print("[OK] Job Silver de orders_clean concluído com sucesso.")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_silver_orders_clean()