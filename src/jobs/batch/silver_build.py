from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.jobs.batch import common
from src.jobs.batch.dq import validate_and_log
from src.jobs.batch.session import build_spark
from src.jobs.streaming.dims import load_dims
from src.jobs.streaming.ensure_tables import (
    TOPIC_EVENTS,
    TOPIC_SHIPMENT,
    TOPIC_TRACKING,
)
from src.jobs.streaming.transform import flag_quality
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def _event_type_norm() -> "F.Column":
    return F.when(
        F.col("event_type") == "shipment_created", F.lit("ORDER_CREATED")
    ).otherwise(F.upper(F.col("event_type")))


def quarantine(df: DataFrame, topic: str, event_type: str) -> DataFrame:
    """Drop rows that fail DQ rules (reuse flag_quality). Sub-events of the shipment
    topic lack the goods columns, so they only get the base null checks."""
    qa_topic = (
        topic
        if (topic != TOPIC_SHIPMENT or event_type == "shipment_created")
        else TOPIC_TRACKING
    )
    flagged = flag_quality(df, qa_topic)
    return flagged.filter("is_valid").drop("dq_flags", "is_valid")


def silver_enrich(df: DataFrame, dims: dict[str, DataFrame]) -> DataFrame:
    """Column-aware dim enrichment: only join a dim when its key column is present.
    Dim-derived columns are renamed to avoid collisions with native event fields."""
    cols = set(df.columns)
    out = df.withColumn("event_type_norm", _event_type_norm())

    if "facility_id" in cols:
        fac = dims["dim_facility"].select(
            "facility_id",
            F.col("branch_id").alias("facility_branch_id"),
            F.col("province_id").alias("facility_province_id"),
        )
        out = out.join(F.broadcast(fac), "facility_id", "left")

    if "route_id" in cols:
        route = dims["dim_route"].select(
            "route_id",
            F.col("destination_hub_id").alias("route_destination_hub_id"),
            F.col("estimated_duration_hours").alias("route_est_duration_hours"),
        )
        out = out.join(F.broadcast(route), "route_id", "left")

    if "service_type_id" in cols:
        svc = dims["dim_service_type"].select(
            "service_type_id", F.col("speed_tier").alias("service_speed_tier")
        )
        out = out.join(F.broadcast(svc), "service_type_id", "left")

    return out.withColumn("silver_loaded_at", F.current_timestamp())


def build_silver(spark: SparkSession, start: str, end: str) -> None:
    logger.info("Starting to build silver table with window [%s, %s)", start, end)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {common.CATALOG}.{common.SILVER_DB}")
    dims = load_dims(spark, common.DIM_DB)
    logger.info("Loading dimensions completed")

    for topic, events in TOPIC_EVENTS.items():
        logger.info("Processing topic %s with events %s", topic, events)
        for event_type in events:
            df = common.read_window(spark, common.BRONZE_DB, event_type, start, end)
            if df is None or df.rdd.isEmpty():
                logger.info("No bronze rows for %s in window; skipping", event_type)
                continue

            validate_and_log(df, topic, event_type, end)

            clean = quarantine(df, topic, event_type).dropDuplicates(["event_id"])
            enriched = silver_enrich(clean, dims)

            common.merge_into(
                spark,
                enriched,
                common.SILVER_DB,
                f"silver_{event_type}",
                key_cols=["event_id"],
                partition_col="event_time",
            )
        logger.info("Processing topic %s completed", topic)
    logger.info("All topics processed successfully")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-start", required=True, help="window start, ISO8601")
    parser.add_argument("--run-end", required=True, help="window end, ISO8601 (exclusive)")
    args = parser.parse_args()
    logger.info("Starting silver build for [%s, %s)", args.run_start, args.run_end)
    spark = build_spark(common.warehouse_path())
    try:
        build_silver(spark, args.run_start, args.run_end)
        logger.info("Silver build complete for [%s, %s)", args.run_start, args.run_end)
    except Exception:
        logger.exception("Silver build failed")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
