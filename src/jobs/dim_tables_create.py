import os

from pyspark.sql import DataFrame, SparkSession

from src.utils.get_parame import ParsParam
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

CATALOG = "glue"


def build_spark(warehouse: str) -> SparkSession:
    """SparkSession wired to the Glue-backed Iceberg catalog `glue`."""
    return (
        SparkSession.builder.appName("dim_tables_create")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(
            f"spark.sql.catalog.{CATALOG}.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog",
        )
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", warehouse)
        .config(
            f"spark.sql.catalog.{CATALOG}.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .getOrCreate()
    )


def read_source(spark: SparkSession, source_format: str, source_path: str) -> DataFrame:
    if source_format == "csv":
        return (
            spark.read.option("header", "true")
            .option("inferSchema", "true")
            .csv(source_path)
        )
    return spark.read.format(source_format).load(source_path)


def create_table(spark: SparkSession, database: str, spec: dict) -> None:
    table = spec["source_table"]
    fqn = f"{CATALOG}.{database}.{table}"

    df = read_source(spark, spec["source_format"], spec["source_path"])
    (
        df.writeTo(fqn)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .tableProperty("write.format.default", "parquet")
        .createOrReplace()
    )
    logger.info("Created %s (%d cols) from %s", fqn, len(df.columns), spec["source_path"])


def main() -> None:
    sources = ParsParam()()["source"]

    warehouse = f"s3a://{os.environ['ICEBERG_BUCKET']}/warehouse/"
    database = os.environ.get("GLUE_DATABASE", "vdt_logistics_dev_gold")

    spark = build_spark(warehouse)
    try:
        for spec in sources:
            create_table(spark, database, spec)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
