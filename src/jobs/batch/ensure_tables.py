from pyspark.sql import SparkSession
import os

from src.jobs.batch.session import build_spark
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

CATALOG = "glue"
GOLD_DB = os.environ.get("GOLD_DATABASE", "vdt_logistics_dev_gold")

def ensure_fact_tables(spark: SparkSession, database: str):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}.{database}")

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS fact_shipment (
            shipment_id string,
            partner_id string,
            service_type_id string,

            sender_province_id string,
            sender_district_id string,
            sender_ward_id string,
            pickup_facility_id string,

            receiver_province_id string,
            receiver_district_id string,
            receiver_ward_id string,
            delivery_facility_id string,

            declared_value_vnd decimal(20,6),
            weight_gram int,
            length_cm double,
            width_cm double,
            height_cm double,
            item_category string,
            is_fragile boolean,

            cod_amount_vnd decimal(20,6),
            shipping_fee_vnd decimal(20,6),
            fuel_surcharge_vnd decimal(20,6),
            remote_area_fee_vnd decimal(20,6),
            insurance_fee_vnd decimal(20,6),
            total_fee_vnd decimal(20,6),
            sla_committed_date string,

            current_status string,
            current_facility_id string,
            assigned_shipper_id string,

            created_date_id int,
            delivered_date_id int,
            returned_date_id int,

            created_at timestamp,
            pickedup_at timestamp,
            origin_post_office_arrived_at timestamp,
            origin_post_office_departed_at timestamp,
            first_hub_arrived_at timestamp,
            last_hub_departed_at timestamp,
            dispatched_for_delivery_at timestamp,
            delivered_at timestamp,
            returned_at timestamp,
            updated_at timestamp,

            is_delayed boolean,
            is_returned boolean,
            is_lost boolean
        )
        USING iceberg
        PARTITIONED BY (days(created_at))
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'format-version' = '2'
        );
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS fact_shipment_route (
            shipment_id string,
            date_id string,
            facility_id string,
            route_id string,
            shipper_id string,
            sequence_no int,
            event_type string,
            event_time timestamp,
            status_before string
        )
        USING iceberg
        PARTITIONED BY (days(event_time))
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'format-version' = '2'
        );
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS fact_delivery_attempt (
            shipment_id string,
            attempt_no int,
            shipper_id string,
            facility_id string,
            date_id int,
            attempt_ts timestamp,
            result string,
            failure_reason_code stringm
            failure_reason_detail stringm
            cod_collected_vnd int
        )
        USING iceberg
        PARTITIONED BY (days(attempt_ts))
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'format-version' = '2'
        );
    """
    )
    
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS fact_hub_inventory (
            date_id int,
            snapshot_ts timestamp,
            facility_id string,
            total_shipments int,
            overdue_shipments int,
            capacity_utilization_pct decimal(5,2)
        )
        USING iceberg
        PARTITIONED BY (days(snapshot_ts))
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'format-version' = '2'
        );
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS fact_financial_transaction(
            shipment_id string,
            transaction_ts timestamp
            facility_id string,
            branch_id string,
            partner_id string,
            service_type_id string,
            revenue_type string,
            revenue_amount_vnd int,
            cod_amount_vnd intm
            collected_at timestamp
        )
        USING iceberg
        PARTITIONED BY (days(transaction_ts))
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'format-version' = '2'
        );
        """
    )

if __name__ == "__main__":
    spark = build_spark(CATALOG)

    try:
        ensure_fact_tables(spark, GOLD_DB)
        logger.info("Successfully created fact tables")
    except Exception:
        logger.exception("Failed to create fact tables")
        raise
    finally:
        spark.stop()