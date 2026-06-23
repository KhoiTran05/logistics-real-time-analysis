from __future__ import annotations

from pyspark.sql import SparkSession

CATALOG = "glue"


def build_spark(warehouse: str) -> SparkSession:
    """SparkSession on the Glue Iceberg catalog, tuned for low-latency streaming."""
    return (
        SparkSession.builder.appName("batch_pipeline")
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

        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
