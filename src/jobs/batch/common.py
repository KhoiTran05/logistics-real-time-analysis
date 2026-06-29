from __future__ import annotations

import os

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from src.utils.logger import setup_logger

logger = setup_logger(__name__)

CATALOG = "glue"

BRONZE_DB = os.environ.get("BRONZE_DATABASE", "vdt_logistics_dev_bronze")
SILVER_DB = os.environ.get("SILVER_DATABASE", "vdt_logistics_dev_silver")
GOLD_DB = os.environ.get("GOLD_DATABASE", "vdt_logistics_dev_gold")
DIM_DB = os.environ.get(
    "DIM_DATABASE", os.environ.get("GLUE_DATABASE", "vdt_logistics_dev_gold")
)


def warehouse_path() -> str:
    return f"s3a://{os.environ['ICEBERG_BUCKET']}/warehouse/"


def date_id(col: Column) -> Column:
    """yyyymmdd integer surrogate key from a timestamp/date column (no dim_date join)."""
    return F.date_format(col, "yyyyMMdd").cast("int")


def read_window(
    spark: SparkSession, database: str, table: str, start: str, end: str, ts_col: str = "event_time"
) -> DataFrame | None:
    """Read one table restricted to [start, end) on its event-time column.

    Returns None when the source table does not exist yet (e.g. an event type that has
    not produced any rows), so callers can simply skip it.
    """
    fqn = f"{CATALOG}.{database}.{table}"
    if not spark.catalog.tableExists(fqn):
        return None
    return spark.table(fqn).where(
        (F.col(ts_col) >= F.to_timestamp(F.lit(start)))
        & (F.col(ts_col) < F.to_timestamp(F.lit(end)))
    )


def table_exists(spark: SparkSession, database: str, table: str) -> bool:
    return spark.catalog.tableExists(f"{CATALOG}.{database}.{table}")


def merge_into(
    spark: SparkSession,
    src_df: DataFrame,
    database: str,
    table: str,
    key_cols: list[str],
    partition_col: str | None = None,
    update_assignments: dict[str, str] | None = None,
) -> None:
    """Idempotent upsert of `src_df` into an Iceberg table via MERGE INTO.

    Creates the target (partitioned by days(partition_col)) on first run when it does
    not already exist — used for the dynamically-shaped Silver tables. Gold fact tables
    are pre-created by ensure_tables, so the create branch is skipped for them.

    `update_assignments` (col -> SQL expr referencing `s.`/`t.`) overrides the matched
    UPDATE for accumulating-snapshot semantics; otherwise UPDATE SET * is used.
    """
    fqn = f"{CATALOG}.{database}.{table}"

    if not spark.catalog.tableExists(fqn):
        writer = src_df.limit(0).writeTo(fqn).using("iceberg").tableProperty(
            "format-version", "2"
        ).tableProperty("write.format.default", "parquet")
        if partition_col:
            writer = writer.partitionedBy(F.days(partition_col))
        writer.create()
        logger.info("Created table %s", fqn)

    view = f"src_{table}"
    src_df.createOrReplaceTempView(view)

    on_clause = " AND ".join(f"t.{k} = s.{k}" for k in key_cols)
    if update_assignments:
        set_clause = ", ".join(f"{c} = {expr}" for c, expr in update_assignments.items())
        matched = f"WHEN MATCHED THEN UPDATE SET {set_clause}"
    else:
        matched = "WHEN MATCHED THEN UPDATE SET *"

    spark.sql(
        f"""
        MERGE INTO {fqn} t
        USING {view} s
        ON {on_clause}
        {matched}
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    logger.info("Merged %d rows into %s", src_df.count(), fqn)
