from __future__ import annotations

import shutil
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp


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
        .appName("retail-lakehouse-silver-order-items-prior-enriched")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_dataset(spark: SparkSession, path: Path, dataset_name: str) -> DataFrame:
    print("==================================================")
    print(f"Leitura do dataset: {dataset_name}")
    print("==================================================")
    print(f"Diretório de entrada: {path}")

    df = spark.read.parquet(str(path))

    print(f"Schema do dataset {dataset_name}:")
    df.printSchema()

    return df


def build_silver_order_items_prior_enriched(
    order_products_prior_df: DataFrame,
    orders_clean_df: DataFrame,
    products_enriched_df: DataFrame,
) -> DataFrame:
    print("==================================================")
    print("Transformação Silver de order_items_prior_enriched")
    print("==================================================")

    silver_df = (
        order_products_prior_df.alias("op")
        .join(
            orders_clean_df.alias("o"),
            on=col("op.order_id") == col("o.order_id"),
            how="inner",
        )
        .join(
            products_enriched_df.alias("p"),
            on=col("op.product_id") == col("p.product_id"),
            how="left",
        )
        .select(
            col("op.order_id").alias("order_id"),
            col("op.product_id").alias("product_id"),
            col("op.add_to_cart_order").alias("add_to_cart_order"),
            col("op.reordered").alias("reordered"),
            col("o.user_id").alias("user_id"),
            col("o.eval_set").alias("eval_set"),
            col("o.order_number").alias("order_number"),
            col("o.order_dow").alias("order_dow"),
            col("o.order_hour_of_day").alias("order_hour_of_day"),
            col("o.days_since_prior_order").alias("days_since_prior_order"),
            col("o.is_first_order").alias("is_first_order"),
            col("p.product_name").alias("product_name"),
            col("p.aisle_id").alias("aisle_id"),
            col("p.aisle").alias("aisle"),
            col("p.department_id").alias("department_id"),
            col("p.department").alias("department"),
            col("op.raw_ingestion_date").alias("bronze_raw_ingestion_date"),
            current_timestamp().alias("silver_processed_at"),
        )
        .filter(col("order_id").isNotNull())
        .filter(col("product_id").isNotNull())
    )

    print("Schema após enriquecimento Silver:")
    silver_df.printSchema()

    return silver_df


def write_silver_order_items_prior_enriched(df: DataFrame, output_dir: Path) -> None:
    print("==================================================")
    print("Escrita da Silver de order_items_prior_enriched")
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

    print("[OK] Silver de order_items_prior_enriched escrita com sucesso em Parquet.")


def summarize_silver_output(df: DataFrame, output_dir: Path) -> None:
    print("==================================================")
    print("Resumo da Silver de order_items_prior_enriched")
    print("==================================================")
    print(f"Total de registros: {df.count()}")
    print(f"Diretório final: {output_dir}")

    print("Prévia dos dados:")
    df.show(20, truncate=False)


def run_silver_order_items_prior_enriched() -> None:
    print("==================================================")
    print("Retail Lakehouse - Silver de order_items_prior_enriched")
    print("==================================================")

    paths = resolve_container_data_paths()

    order_products_prior_dir = paths["bronze_dir"] / "instacart" / "order_products_prior"
    orders_clean_dir = paths["silver_dir"] / "instacart" / "orders_clean"
    products_enriched_dir = paths["silver_dir"] / "instacart" / "products_enriched"

    latest_order_products_prior_partition = find_latest_partition(order_products_prior_dir)
    latest_orders_clean_partition = find_latest_partition(orders_clean_dir)
    latest_products_enriched_partition = find_latest_partition(products_enriched_dir)

    silver_ingestion_date = extract_ingestion_date_from_partition(
        latest_order_products_prior_partition
    )

    output_dir = (
        paths["silver_dir"]
        / "instacart"
        / "order_items_prior_enriched"
        / f"ingestion_date={silver_ingestion_date}"
    )

    spark = create_spark_session()

    try:
        order_products_prior_df = read_dataset(
            spark=spark,
            path=latest_order_products_prior_partition,
            dataset_name="order_products_prior",
        )
        orders_clean_df = read_dataset(
            spark=spark,
            path=latest_orders_clean_partition,
            dataset_name="orders_clean",
        )
        products_enriched_df = read_dataset(
            spark=spark,
            path=latest_products_enriched_partition,
            dataset_name="products_enriched",
        )

        silver_df = build_silver_order_items_prior_enriched(
            order_products_prior_df=order_products_prior_df,
            orders_clean_df=orders_clean_df,
            products_enriched_df=products_enriched_df,
        )
        write_silver_order_items_prior_enriched(df=silver_df, output_dir=output_dir)
        summarize_silver_output(df=silver_df, output_dir=output_dir)

        print("[OK] Job Silver de order_items_prior_enriched concluído com sucesso.")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_silver_order_items_prior_enriched()