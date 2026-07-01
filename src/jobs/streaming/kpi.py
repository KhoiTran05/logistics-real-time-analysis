from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

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
        .withColumn("ingested_at", F.current_timestamp())
        .drop("window")
    )

    string_cols = [f.name for f in out.schema.fields if isinstance(f.dataType, StringType)]
    for c in string_cols:
        out = out.withColumn(c, F.coalesce(F.col(c), F.lit("UNKNOWN")))
        
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
            # Revenue/COD
            F.sum("total_revenue").alias("total_revenue_vnd"),

            F.sum(
                F.when(
                    F.col("event_type") == "cod_collected",
                    F.col("cod_amount_vnd")
                )
                .otherwise(0)
            ).alias("total_cod_vnd"),

            # COD collection success rate
            num.alias("cod_collected_count"),
            denom.alias("cod_committed_count"),
        )

    _to_clickhouse(out, "kpi_financial")

def _shipment_kpi(df: DataFrame) -> None:
    shipment_created = df.filter(F.col("event_type") == "shipment_created")

    # Order volume
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

def _tracking_kpi(df: DataFrame) -> None:
    facility_counter = df \
        .groupBy(
            F.window("event_time", KPI_WINDOW_DURATION),
            "facility_id" 
        ) \
        .agg(
            # Throughput/Backlog
            F.count(
                F.when(F.col("event_type").isin("arrived_at_origin_post_office", "arrived_at_hub", "arrived_at_destination_post_office",), 1)
            ).alias("inbound_count"),
            F.count(
                F.when(F.col("event_type").isin("departed_origin_post_office", "departed_hub", "dispatched_for_delivery"), 1)
            ).alias("outbound_count"),

            # Failed-delivery rate
            F.count(
                F.when(F.col("event_type") == "failed_delivery", 1)
            ).alias("failed_delivery_count"),
            F.count(
                F.when(F.col("event_type") == "dispatched_for_delivery", 1)
            ).alias("out_for_delivery_count"),
        )
    
    # Failure reason
    failure_reason_counter = df \
        .filter(F.col("event_type") == "failed_delivery") \
        .groupBy(
            F.window("event_time", KPI_WINDOW_DURATION),
            "failure_reason_code"
        ) \
        .agg(
            F.count("*").alias("failed_delivery_count"),
        )

    # Global return rate
    returned_counter = df \
        .groupBy(F.window("event_time", KPI_WINDOW_DURATION)) \
        .agg(
            F.count(
                F.when(F.col("event_type") == "returned_to_sender", 1)
            ).alias("returned_count")
        )

    _to_clickhouse(facility_counter, "kpi_facility_flow")
    _to_clickhouse(failure_reason_counter, "kpi_failure_reason")
    _to_clickhouse(returned_counter, "kpi_returns")
    

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
                _tracking_kpi(base_df)
        finally:
            batch_df.unpersist()

    return _write


def start_kpis(enriched: DataFrame, writer, checkpoint: str, name: str):

    return (
        enriched.writeStream.outputMode("append")
        .queryName(name)
        .option("checkpointLocation", checkpoint)
        .foreachBatch(writer)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )
