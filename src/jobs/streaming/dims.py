from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from src.jobs.streaming.config import CATALOG


def load_dims(spark: SparkSession, dim_db: str) -> dict[str, DataFrame]:
    names = [
        "dim_facility", "dim_branch", "dim_province",
        "dim_service_type", "dim_route",
    ]
    return {n: spark.table(f"{CATALOG}.{dim_db}.{n}") for n in names}
