from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

from src.jobs.streaming.config import (
    TOPIC_FINANCIAL,
    TOPIC_SHIPMENT,
    TOPIC_TRACKING,
    WATERMARK,
)


def flag_quality(df: DataFrame, topic: str) -> DataFrame:
    """Add data-quality flags (`dq_flags` / `is_valid`). Non-stateful.

    Dirty rows are *flagged*, not dropped, so downstream DQ KPIs can count them.
    """
    flags = [
        F.when(F.col("event_id").isNull(), F.lit("NULL_EVENT_ID")),
        F.when(F.col("event_time").isNull(), F.lit("NULL_EVENT_TIME")),
        F.when(F.col("shipment_id").isNull(), F.lit("NULL_SHIPMENT_ID")),
    ]
    if topic == TOPIC_SHIPMENT:
        flags += [
            F.when(F.col("weight_gram") <= 0, F.lit("NON_POSITIVE_WEIGHT")),
            F.when(F.col("declared_value_vnd") < 0, F.lit("NEGATIVE_DECLARED_VALUE")),
            F.when(
                F.to_date("sla_committed_date") < F.to_date("event_time"),
                F.lit("SLA_DATE_IN_PAST"),
            ),
        ]

    dq = F.array_compact(F.array(*flags))
    return df.withColumn("dq_flags", dq).withColumn("is_valid", F.size(dq) == 0)


def clean(df: DataFrame, topic: str) -> DataFrame:
    """Watermark + idempotent dedup on event_id, plus data-quality flags.

    Used by the per-topic KPI streams (dedup is safe there: the windowed aggregates run
    in batch mode inside foreachBatch). The unified state machine must NOT chain dedup
    upstream of applyInPandasWithState, so it uses flag_quality directly; exact-once
    dedup on event_id is a Silver/batch concern per schema_design.md.
    """
    deduped = df.withWatermark("event_time", WATERMARK[topic]).dropDuplicates(
        ["event_id"]
    )
    return flag_quality(deduped, topic)


_EVENT_TYPE_NORM = F.when(
    F.col("event_type") == "shipment_created", F.lit("ORDER_CREATED")
).otherwise(F.upper(F.col("event_type")))


def enrich(df: DataFrame, topic: str, dims: dict[str, DataFrame]) -> DataFrame:
    """Broadcast dim joins + derived business fields. Joins are left so no event drops."""
    if topic == TOPIC_FINANCIAL:
        out = df.withColumn(
                "total_revenue",
                F.coalesce(F.col("revenue_amount_vnd"), F.lit(0)) +
                F.coalesce(F.col("adjustment_amount_vnd"), F.lit(0))
            )

        branch = dims["dim_branch"].select("branch_id", "region_id")
        return out.join(broadcast(branch), "branch_id", "left") \
            .withColumn("enriched_at", F.current_timestamp())

    if topic == TOPIC_TRACKING:
        out = df.withColumn("event_type_norm", _EVENT_TYPE_NORM)
        fac = dims["dim_facility"].select(
            F.col("facility_id"), F.col("branch_id"), F.col("province_id"),
        )
        out = out.join(broadcast(fac), "facility_id", "left")

        route = dims["dim_route"].select(
            "route_id", "origin_hub_id", "destination_hub_id",
            "estimated_duration_hours",
        )
        return out.join(broadcast(route), "route_id", "left") \
            .withColumn("enriched_at", F.current_timestamp())

    # TOPIC_SHIPMENT
    out = df.withColumn("event_type_norm", _EVENT_TYPE_NORM)
    fac = dims["dim_facility"].select(
        F.col("facility_id").alias("pickup_facility_id"),
        F.col("branch_id").alias("pickup_branch_id"),
    )
    out = out.join(broadcast(fac), "pickup_facility_id", "left")

    br = dims["dim_branch"].select(
        F.col("branch_id").alias("pickup_branch_id"),
        F.col("region_id").alias("pickup_region_id"),
    )
    out = out.join(broadcast(br), "pickup_branch_id", "left")

    svc = dims["dim_service_type"].select(
        "service_type_id", "speed_tier",
        "sla_inner_city_days", "sla_same_province_days",
        "sla_inter_province_days", "sla_remote_days",
    )
    out = out.join(broadcast(svc), "service_type_id", "left")

    prov = dims["dim_province"].select("province_id", "is_remote", "region_id")
    out = (
        out.join(
            broadcast(prov.select(
                F.col("province_id").alias("sender_province_id"),
                F.col("is_remote").alias("sender_is_remote"),
            )),
            "sender_province_id", "left",
        ).join(
            broadcast(prov.select(
                F.col("province_id").alias("receiver_province_id"),
                F.col("is_remote").alias("receiver_is_remote"),
            )),
            "receiver_province_id", "left",
        )
    )

    volumetric_gram = (
        F.col("length_cm") * F.col("width_cm") * F.col("height_cm") / 5000.0 * 1000.0
    )
    return (
        out.withColumn("volumetric_weight_gram", volumetric_gram.cast("int"))
        .withColumn(
            "charged_weight_gram",
            F.greatest(F.col("weight_gram"), volumetric_gram.cast("int")),
        )
        .withColumn(
            "route_type",
            F.when(
                F.col("sender_is_remote") | F.col("receiver_is_remote"), F.lit("REMOTE")
            )
            .when(
                F.col("sender_province_id") == F.col("receiver_province_id"),
                F.lit("SAME_PROVINCE"),
            )
            .otherwise(F.lit("INTER_PROVINCE")),
        )
        .withColumn("enriched_at", F.current_timestamp())
    )


def build_enriched(df: DataFrame, topic: str, dims: dict[str, DataFrame]) -> DataFrame:
    """Kafka raw stream → enriched streaming DataFrame (watermark preserved)."""
    return enrich(clean(df, topic), topic, dims)
