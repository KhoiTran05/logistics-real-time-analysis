from __future__ import annotations

import argparse
import os

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from src.jobs.batch import common
from src.jobs.batch.ensure_tables import ensure_fact_tables
from src.jobs.batch.session import build_spark
from src.jobs.streaming.dims import load_dims
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

EPOCH = "1970-01-01T00:00:00"
OVERDUE_DWELL_SECONDS = int(os.environ.get("OVERDUE_DWELL_SECONDS", "3600"))

MONEY = ["declared_value_vnd", "cod_amount_vnd", "shipping_fee_vnd",
         "fuel_surcharge_vnd", "remote_area_fee_vnd", "insurance_fee_vnd", "total_fee_vnd"]

# normalized event_type → fact_shipment.current_status enum
STATUS_MAP = {
    "ORDER_CREATED": "CREATED",
    "PICKUP_ASSIGNED": "PICKUP_ASSIGNED",
    "PICKED_UP": "PICKED_UP",
    "ARRIVED_AT_ORIGIN_POST_OFFICE": "AT_ORIGIN_POST_OFFICE",
    "DEPARTED_ORIGIN_POST_OFFICE": "AT_ORIGIN_POST_OFFICE",
    "ARRIVED_AT_HUB": "IN_TRANSIT",
    "SORTED_AT_HUB": "IN_TRANSIT",
    "DEPARTED_HUB": "IN_TRANSIT",
    "ARRIVED_AT_DESTINATION_POST_OFFICE": "AT_DESTINATION_POST_OFFICE",
    "DISPATCHED_FOR_DELIVERY": "OUT_FOR_DELIVERY",
    "DELIVERED": "DELIVERED",
    "FAILED_DELIVERY": "FAILED_DELIVERY",
    "RETURN_INITIATED": "RETURNING",
    "RETURNED_TO_SENDER": "RETURNED",
}

# (fact column, normalized event_type, min|max)
LIFECYCLE = [
    ("created_at", "ORDER_CREATED", "min"),
    ("pickedup_at", "PICKED_UP", "max"),
    ("origin_post_office_arrived_at", "ARRIVED_AT_ORIGIN_POST_OFFICE", "max"),
    ("origin_post_office_departed_at", "DEPARTED_ORIGIN_POST_OFFICE", "max"),
    ("first_hub_arrived_at", "ARRIVED_AT_HUB", "min"),
    ("last_hub_departed_at", "DEPARTED_HUB", "max"),
    ("dispatched_for_delivery_at", "DISPATCHED_FOR_DELIVERY", "max"),
    ("delivered_at", "DELIVERED", "max"),
    ("returned_at", "RETURNED_TO_SENDER", "max"),
]

SHIPMENT_EVENTS = ["shipment_created", "pickup_assigned", "picked_up", "return_initiated"]
TRACKING_EVENTS = [
    "arrived_at_origin_post_office", "departed_origin_post_office", "arrived_at_hub",
    "sorted_at_hub", "departed_hub", "arrived_at_destination_post_office",
    "dispatched_for_delivery", "delivered", "failed_delivery", "returned_to_sender",
]
ARRIVAL_NORMS = [
    "ARRIVED_AT_ORIGIN_POST_OFFICE", "ARRIVED_AT_HUB", "ARRIVED_AT_DESTINATION_POST_OFFICE",
]
DEPARTURE_NORMS = ["DEPARTED_ORIGIN_POST_OFFICE", "DEPARTED_HUB", "DISPATCHED_FOR_DELIVERY"]


def _col_or_null(df: DataFrame, name: str, cast: str = "string") -> Column:
    return F.col(name) if name in df.columns else F.lit(None).cast(cast)


def _read(spark: SparkSession, event_type: str, start: str, end: str) -> DataFrame | None:
    return common.read_window(spark, common.SILVER_DB, f"silver_{event_type}", start, end)


def _status_expr(col: Column) -> Column:
    mapping = F.create_map([F.lit(x) for kv in STATUS_MAP.items() for x in kv])
    return F.coalesce(mapping[col], col)


# fact_shipment (Accumulating Snapshot)

def build_fact_shipment(spark: SparkSession, start: str, end: str) -> None:
    parts = []
    for et in SHIPMENT_EVENTS + TRACKING_EVENTS:
        df = _read(spark, et, start, end)
        if df is None:
            continue
        parts.append(
            df.select(
                "shipment_id",
                "event_type_norm",
                "event_time",
                F.coalesce(
                    _col_or_null(df, "facility_id"),
                    _col_or_null(df, "pickup_facility_id"),
                ).alias("facility_id"),
                F.coalesce(
                    _col_or_null(df, "shipper_id"),
                    _col_or_null(df, "assigned_shipper_id"),
                ).alias("shipper_id"),
            )
        )
    if not parts:
        logger.info("fact_shipment: no source events in window")
        return

    evt = parts[0]
    for p in parts[1:]:
        evt = evt.unionByName(p)

    aggs = []
    for col, norm, fn in LIFECYCLE:
        e = F.when(F.col("event_type_norm") == norm, F.col("event_time"))
        aggs.append((F.min(e) if fn == "min" else F.max(e)).alias(col))

    aggs.append(F.max("shipper_id").alias("assigned_shipper_id"))
    aggs.append(
        F.max(
            F.struct(
                F.col("event_time").alias("t"),
                F.col("event_type_norm").alias("etn"),
                F.col("facility_id").alias("fac"),
            )
        ).alias("_latest")
    )
    agg = evt.groupBy("shipment_id").agg(*aggs)

    agg = (
        agg.withColumn("current_status", _status_expr(F.col("_latest.etn")))
        .withColumn("current_facility_id", F.col("_latest.fac"))
        .withColumn("updated_at", F.col("_latest.t"))
        .drop("_latest")
    )

    created = _read(spark, "shipment_created", start, end)
    static_cols = [
        "shipment_id", "partner_id", "service_type_id",
        "sender_province_id", "sender_district_id", "sender_ward_id", "pickup_facility_id",
        "receiver_province_id", "receiver_district_id", "receiver_ward_id", "delivery_facility_id",
        "weight_gram", "length_cm", "width_cm", "height_cm", "item_category", "is_fragile",
        "sla_committed_date",
    ] + MONEY
    if created is not None:
        present = [c for c in static_cols if c in created.columns]
        agg = agg.join(created.select(*present).dropDuplicates(["shipment_id"]),
                       "shipment_id", "left")

    for c in static_cols:
        if c not in agg.columns:
            agg = agg.withColumn(c, F.lit(None))
    for c in MONEY:
        agg = agg.withColumn(c, F.col(c).cast("decimal(20,6)"))
    agg = agg.withColumn("weight_gram", F.col("weight_gram").cast("int"))

    out = (
        agg.withColumn("created_date_id", common.date_id(F.col("created_at")))
        .withColumn("delivered_date_id", common.date_id(F.col("delivered_at")))
        .withColumn("returned_date_id", common.date_id(F.col("returned_at")))
        .withColumn("is_returned", F.col("returned_at").isNotNull())
        .withColumn("is_lost", F.lit(False))
        .withColumn(
            "is_delayed",
            F.col("delivered_at").isNotNull()
            & F.col("sla_committed_date").isNotNull()
            & (F.to_date("sla_committed_date") < F.to_date("delivered_at")),
        )
    )

    fs_cols = [
        "shipment_id", "partner_id", "service_type_id",
        "sender_province_id", "sender_district_id", "sender_ward_id", "pickup_facility_id",
        "receiver_province_id", "receiver_district_id", "receiver_ward_id", "delivery_facility_id",
        "declared_value_vnd", "weight_gram", "length_cm", "width_cm", "height_cm",
        "item_category", "is_fragile",
        "cod_amount_vnd", "shipping_fee_vnd", "fuel_surcharge_vnd", "remote_area_fee_vnd",
        "insurance_fee_vnd", "total_fee_vnd", "sla_committed_date",
        "current_status", "current_facility_id", "assigned_shipper_id",
        "created_date_id", "delivered_date_id", "returned_date_id",
        "created_at", "pickedup_at", "origin_post_office_arrived_at",
        "origin_post_office_departed_at", "first_hub_arrived_at", "last_hub_departed_at",
        "dispatched_for_delivery_at", "delivered_at", "returned_at", "updated_at",
        "is_delayed", "is_returned", "is_lost",
    ]
    out = out.select(*fs_cols)

    # Accumulating-snapshot MERGE: never null-out an already-set field; keep the
    # earliest arrival / latest departure / latest status across windows.
    keep = lambda c: f"coalesce(t.{c}, s.{c})"
    upd = {c: keep(c) for c in fs_cols}
    upd["created_at"] = "least(t.created_at, s.created_at)"
    upd["first_hub_arrived_at"] = "least(t.first_hub_arrived_at, s.first_hub_arrived_at)"
    upd["last_hub_departed_at"] = "greatest(t.last_hub_departed_at, s.last_hub_departed_at)"
    upd["updated_at"] = "greatest(t.updated_at, s.updated_at)"
    for c in ("current_status", "current_facility_id"):
        upd[c] = f"case when s.updated_at >= t.updated_at then s.{c} else t.{c} end"
    upd["is_returned"] = "t.is_returned or s.is_returned"
    upd["is_lost"] = "t.is_lost or s.is_lost"
    upd["is_delayed"] = (
        "case when coalesce(s.delivered_at, t.delivered_at) is not null "
        "and coalesce(s.sla_committed_date, t.sla_committed_date) is not null "
        "and to_date(coalesce(s.sla_committed_date, t.sla_committed_date)) "
        "< to_date(coalesce(s.delivered_at, t.delivered_at)) then true else false end"
    )
    upd.pop("shipment_id", None)

    common.merge_into(spark, out, common.GOLD_DB, "fact_shipment",
                      key_cols=["shipment_id"], update_assignments=upd)


# fact_shipment_route (Transaction)

def build_fact_shipment_route(spark: SparkSession, start: str, end: str) -> None:
    parts = []
    for et in SHIPMENT_EVENTS + TRACKING_EVENTS:
        df = _read(spark, et, start, end)
        if df is None:
            continue
        parts.append(
            df.select(
                F.col("event_id"),
                F.col("shipment_id"),
                common.date_id(F.col("event_time")).alias("date_id"),
                F.coalesce(
                    _col_or_null(df, "facility_id"), _col_or_null(df, "pickup_facility_id")
                ).alias("facility_id"),
                _col_or_null(df, "route_id").alias("route_id"),
                F.coalesce(
                    _col_or_null(df, "shipper_id"), _col_or_null(df, "assigned_shipper_id")
                ).alias("shipper_id"),
                _col_or_null(df, "sequence_no", "int").cast("int").alias("sequence_no"),
                F.col("event_type_norm").alias("event_type"),
                F.col("event_time")
            )
        )
    if not parts:
        return
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    out = out.dropDuplicates(["event_id"])
    common.merge_into(spark, out, common.GOLD_DB, "fact_shipment_route",
                      key_cols=["event_id"], partition_col="event_time")


# fact_delivery_attempt (Transaction)

def build_fact_delivery_attempt(spark: SparkSession, start: str, end: str) -> None:
    delivered = _read(spark, "delivered", start, end)
    failed = _read(spark, "failed_delivery", start, end)

    def proj(df: DataFrame, result: str) -> DataFrame:
        return df.select(
            "shipment_id",
            F.col("attempt_no").cast("int").alias("attempt_no"),
            "shipper_id",
            "facility_id",
            common.date_id(F.col("event_time")).alias("date_id"),
            F.col("event_time").alias("attempt_ts"),
            F.lit(result).alias("result"),
            _col_or_null(df, "failure_reason_code").alias("failure_reason_code"),
            _col_or_null(df, "failure_reason_detail").alias("failure_reason_detail"),
        )

    parts = []
    if delivered is not None:
        parts.append(proj(delivered, "SUCCESS"))
    if failed is not None:
        parts.append(proj(failed, "FAILED"))
    if not parts:
        return

    attempts = parts[0]
    for p in parts[1:]:
        attempts = attempts.unionByName(p)

    cod = _read(spark, "cod_collected", start, end)
    if cod is not None:
        cod = cod.select(
            "shipment_id",
            F.col("attempt_no").cast("int").alias("attempt_no"),
            F.col("cod_amount_vnd").cast("bigint").alias("cod_collected_vnd"),
        ).dropDuplicates(["shipment_id", "attempt_no"])
        attempts = attempts.join(cod, ["shipment_id", "attempt_no"], "left")
    else:
        attempts = attempts.withColumn("cod_collected_vnd", F.lit(None).cast("bigint"))

    out = attempts.withColumn(
        "cod_collected_vnd", F.coalesce(F.col("cod_collected_vnd"), F.lit(0)).cast("bigint")
    ).dropDuplicates(["shipment_id", "attempt_no"])

    common.merge_into(spark, out, common.GOLD_DB, "fact_delivery_attempt",
                      key_cols=["shipment_id", "attempt_no"], partition_col="attempt_ts")


# fact_financial_transaction (Transaction)

def build_fact_financial_transaction(spark: SparkSession, start: str, end: str) -> None:
    specs = {
        "shipping_fee_confirmed": ("SHIPPING_FEE", "revenue_amount_vnd", None),
        "cod_collected": ("COD_COLLECTION", None, "cod_amount_vnd"),
        "cod_failed": ("COD_FAILED", None, None),
        "fee_adjusted": ("FEE_ADJUSTMENT", "adjustment_amount_vnd", None),
    }
    parts = []
    for et, (rev_type, rev_col, cod_col) in specs.items():
        df = _read(spark, et, start, end)
        if df is None:
            continue
        rev = _col_or_null(df, rev_col, "bigint") if rev_col else F.lit(0)
        cod = _col_or_null(df, cod_col, "bigint") if cod_col else F.lit(0)
        collected = F.col("event_time") if et == "cod_collected" else F.lit(None).cast("timestamp")
        parts.append(
            df.select(
                "event_id", "shipment_id",
                F.col("event_time").alias("transaction_ts"),
                "facility_id", "branch_id", "partner_id", "service_type_id",
                F.lit(rev_type).alias("revenue_type"),
                rev.cast("bigint").alias("revenue_amount_vnd"),
                cod.cast("bigint").alias("cod_amount_vnd"),
                collected.alias("collected_at"),
            )
        )
    if not parts:
        return
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    out = out.dropDuplicates(["event_id"])
    common.merge_into(spark, out, common.GOLD_DB, "fact_financial_transaction",
                      key_cols=["event_id"], partition_col="transaction_ts")


# fact_hub_inventory (Periodic Snapshot)

def build_fact_hub_inventory(spark: SparkSession, end: str, dims: dict[str, DataFrame]) -> None:
    """Snapshot of net facility occupancy at the window end (full history up to `end`)."""
    parts = []
    for et in TRACKING_EVENTS:
        df = common.read_window(spark, common.SILVER_DB, f"silver_{et}", EPOCH, end)
        if df is None or "facility_id" not in df.columns:
            continue
        sign = F.when(F.col("event_type_norm").isin(ARRIVAL_NORMS), F.lit(1)).when(
            F.col("event_type_norm").isin(DEPARTURE_NORMS), F.lit(-1)
        ).otherwise(F.lit(0))
        parts.append(
            df.select("facility_id", "shipment_id", "event_time", sign.alias("sign")).where(
                F.col("sign") != 0
            )
        )
    if not parts:
        return
    moves = parts[0]
    for p in parts[1:]:
        moves = moves.unionByName(p)

    per_pkg = moves.groupBy("facility_id", "shipment_id").agg(
        F.sum("sign").alias("net"),
    )
    present = per_pkg.where(F.col("net") > 0)

    snapshot = F.to_timestamp(F.lit(end))

    inv = present.groupBy("facility_id").agg(
        F.count("*").cast("int").alias("total_shipments"),
    )

    cap = dims["dim_facility"].select(
        "facility_id", F.col("capacity_per_day").cast("double").alias("cap")
    )
    out = (
        inv.join(F.broadcast(cap), "facility_id", "left")
        .withColumn("snapshot_ts", snapshot)
        .withColumn("date_id", common.date_id(snapshot))
        .withColumn(
            "capacity_utilization_pct",
            F.when(
                (F.col("cap").isNotNull()) & (F.col("cap") > 0),
                F.least(F.round(F.col("total_shipments") / F.col("cap") * 100, 2), F.lit(999.99)),
            ).cast("decimal(5,2)"),
        )
        .select("date_id", "snapshot_ts", "facility_id",
                "total_shipments", "capacity_utilization_pct")
    )

    common.merge_into(spark, out, common.GOLD_DB, "fact_hub_inventory",
                      key_cols=["facility_id", "snapshot_ts"], partition_col="snapshot_ts")


def build_gold(spark: SparkSession, start: str, end: str) -> None:
    ensure_fact_tables(spark, common.GOLD_DB)
    dims = load_dims(spark, common.DIM_DB)

    build_fact_shipment(spark, start, end)
    build_fact_shipment_route(spark, start, end)
    build_fact_delivery_attempt(spark, start, end)
    build_fact_financial_transaction(spark, start, end)
    build_fact_hub_inventory(spark, end, dims)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-start", required=True)
    parser.add_argument("--run-end", required=True)
    args = parser.parse_args()

    spark = build_spark(common.warehouse_path())
    try:
        build_gold(spark, args.run_start, args.run_end)
        logger.info("Gold build complete for [%s, %s)", args.run_start, args.run_end)
    except Exception:
        logger.exception("Gold build failed")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
