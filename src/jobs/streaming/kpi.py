from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.jobs.streaming.config import (
    CLICKHOUSE_DRIVER,
    CLICKHOUSE_JDBC_URL,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_USER,
    KPI_WINDOW_DURATION,
    KPI_WINDOW_SLIDING_DURATION,
    TOPIC_FINANCIAL,
    TOPIC_SHIPMENT,
    TOPIC_TRACKING,
    TRIGGER_INTERVAL,
)
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def _to_clickhouse(df: DataFrame, table: str) -> None:
    """Append a per-batch windowed aggregate to a ClickHouse table.

    The `window` struct is flattened to `window_start` / `window_end`. Tables are
    SummingMergeTree: partial per-batch aggregates for the same window are summed
    by the engine, so queries must aggregate (SUM / GROUP BY), never read raw rows.
    """
    out = (
        df.withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .drop("window")
    )
    (
        out.write.format("jdbc")
        .option("url", CLICKHOUSE_JDBC_URL)
        .option("dbtable", table)
        .option("user", CLICKHOUSE_USER)
        .option("password", CLICKHOUSE_PASSWORD)
        .option("driver", CLICKHOUSE_DRIVER)
        .mode("append")
        .save()
    )


def _financial_kpi(df: DataFrame) -> None:

    denom = F.count(
        F.when(
            F.col("event_type").isin("cod_collected", "cod_failed"),
            1
        )
    )

    num = F.count(
        F.when(
            F.col("event_type") == "cod_collected",
            1
        )
    )

    out = df \
        .groupBy(
            F.window(
                "event_time",
                KPI_WINDOW_DURATION,
                KPI_WINDOW_SLIDING_DURATION
            ),
            "facility_id"
        ) \
        .agg(
            F.sum("total_revenue").alias("total_revenue_vnd"),

            F.sum(
                F.when(
                    F.col("event_type") == "cod_collected",
                    F.col("cod_amount_vnd")
                )
                .otherwise(0)
            ).alias("total_cod_vnd"),

            num.alias("cod_collected_count"),
            denom.alias("cod_committed_count"),
        )

    _to_clickhouse(out, "kpi_financial")

def _shipment_kpi(df: DataFrame) -> None:
    shipment_created = df.filter(F.col("event_type") == "shipment_created")

    order_facility = shipment_created \
        .groupBy(
            F.window("event_time", KPI_WINDOW_DURATION),
            "pickup_facility_id"
        ) \
        .agg(
            F.count("*").alias("order_count"),
        )

    order_partner_service = shipment_created \
        .groupBy(
            F.window("event_time", KPI_WINDOW_DURATION),
            "partner_id",
            "service_type_id"
        ) \
        .agg(
            F.count("*").alias("order_count"),
        )

    _to_clickhouse(order_facility, "kpi_order_volume_facility")
    _to_clickhouse(order_partner_service, "kpi_order_volume_partner_service")


def kpi_batch_writer(topic: str):

    def _log_batch(batch_df: DataFrame, batch_id: int) -> None:
        stats = batch_df.agg(
            F.count(F.lit(1)).alias("total"),
            F.count(F.when(~F.col("is_valid"), 1)).alias("invalid"),
        ).first()
        logger.info(
            "[%s] batch=%d enriched_rows=%d invalid_rows=%d",
            topic, batch_id, stats["total"], stats["invalid"],
        )

    def _write(batch_df: DataFrame, batch_id: int) -> None:
        batch_df.persist()
        try:
            _log_batch(batch_df, batch_id)

            base_df = batch_df.filter(F.col("is_valid"))
            if topic == TOPIC_FINANCIAL:
                _financial_kpi(base_df)
            elif topic == TOPIC_SHIPMENT:
                _shipment_kpi(base_df)
            elif topic == TOPIC_TRACKING:
                pass
        finally:
            batch_df.unpersist()

    return _write


def start_kpis(enriched: DataFrame, writer, checkpoint: str):

    return (
        enriched.writeStream.outputMode("append")
        .option("checkpointLocation", checkpoint)
        .foreachBatch(writer)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )
