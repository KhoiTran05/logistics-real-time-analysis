from __future__ import annotations

from typing import Iterator

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.jobs.streaming.config import (
    CLICKHOUSE_DRIVER,
    CLICKHOUSE_JDBC_URL,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_USER,
    TRIGGER_INTERVAL,
    WM_UNIFIED,
)
from src.jobs.streaming.shipment_state import (
    OUTPUT_FIELDS,
    STATE_FIELDS,
    process_batch,
)

UNIFIED_COLS: list[tuple[str, object]] = [
    ("shipment_id", StringType()),
    ("event_type", StringType()),
    ("event_time", TimestampType()),
    ("sequence_no", IntegerType()),
    ("facility_id", StringType()),
    ("pickup_facility_id", StringType()),
    ("route_id", StringType()),
    ("attempt_no", IntegerType()),
    ("service_type_id", StringType()),
    ("sla_committed_date", StringType()),
]
EVENT_COLS = [name for name, _ in UNIFIED_COLS if name != "shipment_id"]

STATE_SCHEMA = StructType([
    StructField("created_at", LongType()),
    StructField("picked_up_at", LongType()),
    StructField("origin_po_arrived_at", LongType()),
    StructField("origin_po_departed_at", LongType()),
    StructField("last_dispatch_at", LongType()),
    StructField("current_hub_id", StringType()),
    StructField("current_hub_arrived_at", LongType()),
    StructField("total_hub_dwell_s", DoubleType()),
    StructField("last_event_time", LongType()),
    StructField("last_event_type", StringType()),
    StructField("current_facility", StringType()),
    StructField("failure_count", IntegerType()),
    StructField("max_attempt_no", IntegerType()),
    StructField("speed_tier", StringType()),
    StructField("sla_committed_date", StringType()),
    StructField("stuck_emitted", BooleanType()),
    StructField("terminated", BooleanType()),
])

OUTPUT_SCHEMA = StructType([
    StructField("record_type", StringType()),
    StructField("shipment_id", StringType()),
    StructField("status", StringType()),
    StructField("current_facility", StringType()),
    StructField("e2e_transit_s", DoubleType()),
    StructField("pickup_lead_s", DoubleType()),
    StructField("first_mile_s", DoubleType()),
    StructField("origin_po_dwell_s", DoubleType()),
    StructField("total_hub_dwell_s", DoubleType()),
    StructField("last_mile_s", DoubleType()),
    StructField("sla_committed_date", StringType()),
    StructField("delivered_date", StringType()),
    StructField("sla_met", IntegerType()),
    StructField("sla_breach", IntegerType()),
    StructField("attempt_count", IntegerType()),
    StructField("first_attempt_success", IntegerType()),
    StructField("is_redelivery", IntegerType()),
    StructField("updated_at", TimestampType()),
    StructField("facility_id", StringType()),
    StructField("anomaly_type", StringType()),
    StructField("severity", StringType()),
    StructField("detail", StringType()),
    StructField("detected_at", TimestampType()),
])

JOURNEY_COLS = [
    "shipment_id", "status", "current_facility", "e2e_transit_s", "pickup_lead_s",
    "first_mile_s", "origin_po_dwell_s", "total_hub_dwell_s", "last_mile_s",
    "sla_committed_date", "delivered_date", "sla_met", "sla_breach",
    "attempt_count", "first_attempt_success", "is_redelivery", "updated_at",
]
ANOMALY_COLS = ["detected_at", "shipment_id", "facility_id", "anomaly_type", "severity", "detail"]
_TS_OUT_COLS = ["updated_at", "detected_at"]


def build_unified(cleaned_by_topic: dict[str, DataFrame]) -> DataFrame:
    """Project each cleaned (valid-only) topic stream to UNIFIED_COLS and unionByName."""
    parts = []
    for df in cleaned_by_topic.values():
        cols = [
            (F.col(name) if name in df.columns else F.lit(None).cast(dtype)).alias(name)
            for name, dtype in UNIFIED_COLS
        ]
        parts.append(df.select(*cols))

    union = parts[0]
    for p in parts[1:]:
        union = union.unionByName(p)
    return union.withWatermark("event_time", WM_UNIFIED)


def _ms(ts) -> int | None:
    return None if ts is None or pd.isna(ts) else int(ts.value // 1_000_000)


def _int(v) -> int | None:
    return None if v is None or pd.isna(v) else int(v)


def _str(v) -> str | None:
    return v if isinstance(v, str) else None


def _events_from_pdf(pdf: pd.DataFrame) -> list[dict]:
    out = []
    for row in pdf.itertuples(index=False):
        d = row._asdict()
        out.append({
            "event_type": _str(d.get("event_type")),
            "event_time": _ms(d.get("event_time")),
            "sequence_no": _int(d.get("sequence_no")),
            "facility_id": _str(d.get("facility_id")),
            "pickup_facility_id": _str(d.get("pickup_facility_id")),
            "route_id": _str(d.get("route_id")),
            "attempt_no": _int(d.get("attempt_no")),
            "service_type_id": _str(d.get("service_type_id")),
            "sla_committed_date": _str(d.get("sla_committed_date")),
        })
    return out


def _rows_to_pdf(rows: list[dict]) -> pd.DataFrame:
    pdf = pd.DataFrame(rows, columns=OUTPUT_FIELDS)
    for c in _TS_OUT_COLS:
        pdf[c] = pd.to_datetime(pdf[c], unit="ms")
    return pdf


def make_fold(route_dest: dict[str, str], speed_tier: dict[str, str]):
    """Build the applyInPandasWithState fold, closing over broadcast dim lookups."""

    def fold(key, pdf_iter: Iterator[pd.DataFrame], state: GroupState) -> Iterator[pd.DataFrame]:
        sid = key[0]
        if state.hasTimedOut:
            st = dict(zip(STATE_FIELDS, state.get))
            rows, new_st, timeout = process_batch(sid, st, [], True, route_dest, speed_tier)
        else:
            st = dict(zip(STATE_FIELDS, state.get)) if state.exists else None
            events = _events_from_pdf(pd.concat(list(pdf_iter), ignore_index=True))
            rows, new_st, timeout = process_batch(sid, st, events, False, route_dest, speed_tier)

        if new_st is None:
            state.remove()
        else:
            state.update(tuple(new_st[f] for f in STATE_FIELDS))
            if timeout is not None:
                wm = state.getCurrentWatermarkMs()
                state.setTimeoutTimestamp(max(timeout, wm + 1000))

        if rows:
            yield _rows_to_pdf(rows)

    return fold
    

def _to_clickhouse(df: DataFrame, table: str) -> None:
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for c in string_cols:
        df = df.withColumn(c, F.coalesce(F.col(c), F.lit("UNKNOWN")))
    (
        df.write.format("jdbc")
        .option("url", CLICKHOUSE_JDBC_URL)
        .option("dbtable", table)
        .option("user", CLICKHOUSE_USER)
        .option("password", CLICKHOUSE_PASSWORD)
        .option("driver", CLICKHOUSE_DRIVER)
        .mode("append")
        .save()
    )


def stateful_batch_writer(batch_df: DataFrame, batch_id: int) -> None:
    batch_df.persist()
    try:
        journey = batch_df.filter(F.col("record_type") == "SHIPMENT").select(*JOURNEY_COLS)
        _to_clickhouse(journey, "kpi_shipment_journey")

        anomalies = batch_df.filter(F.col("record_type") == "ANOMALY").select(*ANOMALY_COLS)
        _to_clickhouse(anomalies, "anomaly_alerts")
    finally:
        batch_df.unpersist()


def start_stateful(unified: DataFrame, route_dest: dict, speed_tier: dict, checkpoint: str):
    stated = unified.groupBy("shipment_id").applyInPandasWithState(
        make_fold(route_dest, speed_tier),
        OUTPUT_SCHEMA,
        STATE_SCHEMA,
        "update",
        GroupStateTimeout.EventTimeTimeout,
    )
    return (
        stated.writeStream.outputMode("update")
        .option("checkpointLocation", checkpoint)
        .foreachBatch(stateful_batch_writer)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )
