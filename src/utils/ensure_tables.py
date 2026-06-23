from __future__ import annotations
from pyspark.sql import functions as F
from pyspark.sql import DataFrame, SparkSession
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

from src.utils.logger import setup_logger

logger = setup_logger(__name__)

CATALOG = "glue"

TOPIC_SHIPMENT = "logistics.shipment.events"
TOPIC_TRACKING = "logistics.tracking.events"
TOPIC_FINANCIAL = "logistics.financial.events"

ENVELOPE_COLS = [
    "event_id",
    "event_type",
    "event_time",
    "processing_time",
    "schema_version",
    "shipment_id",
]

COMMON_FIELDS = {
    TOPIC_SHIPMENT: [],
    TOPIC_TRACKING: ["facility_id", "facility_type", "sequence_no"],
    TOPIC_FINANCIAL: ["facility_id", "branch_id", "partner_id", "service_type_id"],
}

EVENT_FIELDS = {
    # logistics.shipment.events
    "shipment_created": [
        "partner_id", "service_type_id",
        "sender_province_id", "sender_district_id", "sender_ward_id", "pickup_facility_id",
        "receiver_province_id", "receiver_district_id", "receiver_ward_id", "delivery_facility_id",
        "declared_value_vnd", "weight_gram", "length_cm", "width_cm", "height_cm",
        "item_category", "is_fragile",
        "cod_amount_vnd", "shipping_fee_vnd", "fuel_surcharge_vnd", "remote_area_fee_vnd",
        "insurance_fee_vnd", "total_fee_vnd", "payment_by", "sla_committed_date",
    ],
    "pickup_assigned": ["pickup_facility_id", "assigned_shipper_id"],
    "picked_up": ["pickup_facility_id", "shipper_id", "reweighed_gram"],
    "return_initiated": ["facility_id", "reason_code"],

    # logistics.tracking.events 
    "arrived_at_origin_post_office": [],
    "departed_origin_post_office": ["route_id"],
    "arrived_at_hub": ["route_id"],
    "sorted_at_hub": [],
    "departed_hub": ["route_id", "destination_facility_id"],
    "arrived_at_destination_post_office": [],
    "dispatched_for_delivery": ["shipper_id", "attempt_no"],
    "delivered": ["shipper_id", "attempt_no", "recipient_relation"],
    "failed_delivery": ["shipper_id", "attempt_no", "failure_reason_code", "failure_reason_detail"],
    "returned_to_sender": [],

    # logistics.financial.events
    "shipping_fee_confirmed": [
        "revenue_type", "revenue_amount_vnd", "shipping_fee_vnd",
        "fuel_surcharge_vnd", "remote_area_fee_vnd", "insurance_fee_vnd",
    ],
    "cod_collected": ["shipper_id", "attempt_no", "cod_amount_vnd"],
    "cod_failed": ["shipper_id", "attempt_no", "cod_amount_vnd", "reason_code"],
    "fee_adjusted": ["old_total_fee_vnd", "new_total_fee_vnd", "adjustment_amount_vnd", "reason_code"],
}

TOPIC_EVENTS = {
    TOPIC_SHIPMENT: ["shipment_created", "pickup_assigned", "picked_up", "return_initiated"],
    TOPIC_TRACKING: [
        "arrived_at_origin_post_office", "departed_origin_post_office",
        "arrived_at_hub", "sorted_at_hub", "departed_hub",
        "arrived_at_destination_post_office", "dispatched_for_delivery",
        "delivered", "failed_delivery", "returned_to_sender",
    ],
    TOPIC_FINANCIAL: ["shipping_fee_confirmed", "cod_collected", "cod_failed", "fee_adjusted"],
}

KAFKA_META_COLS = ["kafka_partition", "kafka_offset", "ingest_time"]

ENVELOPE_SCHEMA = [
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("processing_time", TimestampType(), True),
    StructField("schema_version", StringType(), True),
    StructField("shipment_id", StringType(), True),
]

def _schema(*fields: StructField) -> StructType:
    return StructType(ENVELOPE_SCHEMA + list(fields))

SCHEMA = {
    TOPIC_SHIPMENT: _schema(
        StructField("partner_id", StringType()),
        StructField("service_type_id", StringType()),
        StructField("sender_province_id", StringType()),
        StructField("sender_district_id", StringType()),
        StructField("sender_ward_id", StringType()),
        StructField("pickup_facility_id", StringType()),
        StructField("receiver_province_id", StringType()),
        StructField("receiver_district_id", StringType()),
        StructField("receiver_ward_id", StringType()),
        StructField("delivery_facility_id", StringType()),
        StructField("declared_value_vnd", LongType()),
        StructField("weight_gram", IntegerType()),
        StructField("length_cm", DoubleType()),
        StructField("width_cm", DoubleType()),
        StructField("height_cm", DoubleType()),
        StructField("item_category", StringType()),
        StructField("is_fragile", BooleanType()),
        StructField("cod_amount_vnd", LongType()),
        StructField("shipping_fee_vnd", LongType()),
        StructField("fuel_surcharge_vnd", LongType()),
        StructField("remote_area_fee_vnd", LongType()),
        StructField("insurance_fee_vnd", LongType()),
        StructField("total_fee_vnd", LongType()),
        StructField("payment_by", StringType()),
        StructField("sla_committed_date", StringType()),
        # pickup_assigned / picked_up / return_initiated
        StructField("assigned_shipper_id", StringType()),
        StructField("shipper_id", StringType()),
        StructField("reweighed_gram", IntegerType()),
        StructField("facility_id", StringType()),
        StructField("reason_code", StringType()),
    ),
    TOPIC_TRACKING: _schema(
        StructField("facility_id", StringType()),
        StructField("facility_type", StringType()),
        StructField("sequence_no", IntegerType()),
        StructField("route_id", StringType()),
        StructField("destination_facility_id", StringType()),
        StructField("shipper_id", StringType()),
        StructField("attempt_no", IntegerType()),
        StructField("recipient_relation", StringType()),
        StructField("failure_reason_code", StringType()),
        StructField("failure_reason_detail", StringType()),
    ),
    TOPIC_FINANCIAL: _schema(
        StructField("facility_id", StringType()),
        StructField("branch_id", StringType()),
        StructField("partner_id", StringType()),
        StructField("service_type_id", StringType()),
        StructField("revenue_type", StringType()),
        StructField("revenue_amount_vnd", LongType()),
        StructField("shipping_fee_vnd", LongType()),
        StructField("fuel_surcharge_vnd", LongType()),
        StructField("remote_area_fee_vnd", LongType()),
        StructField("insurance_fee_vnd", LongType()),
        StructField("shipper_id", StringType()),
        StructField("attempt_no", IntegerType()),
        StructField("cod_amount_vnd", LongType()),
        StructField("reason_code", StringType()),
        StructField("old_total_fee_vnd", LongType()),
        StructField("new_total_fee_vnd", LongType()),
        StructField("adjustment_amount_vnd", LongType()),
    ),
}

def bronze_columns(topic: str, event_type: str) -> list[str]:
    """Exact column projection for an event type's Bronze table."""
    return (ENVELOPE_COLS + COMMON_FIELDS[topic]
            + EVENT_FIELDS[event_type] + KAFKA_META_COLS)
    
def bronze_schema(topic: str, event_type: str) -> StructType:
    """Generate the full schema for an event type's Bronze table."""
    schema_mapping = { field.name : field.dataType for field in SCHEMA[topic].fields}
    fixed = {
        "kafka_partition": IntegerType(),
        "kafka_offset": IntegerType(),
        "ingest_time": TimestampType()
    }

    fields = []
    for col in bronze_columns(topic, event_type):
        if col in fixed:
            fields.append(StructField(col, fixed[col]))
        else:
            fields.append(StructField(col, schema_mapping[col]))
            
    return StructType(fields)

def ensure_bronze_tables(spark: SparkSession, database: str) -> None:
    """Pre-create every per-event-type Bronze table so foreachBatch only appends."""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}.{database}")
    for topic, events in TOPIC_EVENTS.items():
        for event_type in events:
            fqn = f"{CATALOG}.{database}.{event_type}"
            if spark.catalog.tableExists(fqn):
                continue
            (
                spark.createDataFrame([], bronze_schema(topic, event_type))
                .writeTo(fqn)
                .using("iceberg")
                .tableProperty("format-version", "2")
                .tableProperty("write.format.default", "parquet")
                .partitionedBy(F.days("event_time"))
                .create()
            )
            logger.info("Created bronze table %s", fqn)