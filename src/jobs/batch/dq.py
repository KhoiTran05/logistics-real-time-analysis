from __future__ import annotations

import json
import os

import boto3
from pyspark.sql import DataFrame

from src.jobs.streaming.ensure_tables import TOPIC_SHIPMENT
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def _expectations(ds, topic: str, cols: set[str]) -> None:
    """Declare expectations on a SparkDFDataset. Mirrors flag_quality's rules."""
    ds.expect_column_values_to_not_be_null("event_id")
    ds.expect_column_values_to_not_be_null("event_time")
    ds.expect_column_values_to_not_be_null("shipment_id")
    if topic == TOPIC_SHIPMENT and "weight_gram" in cols:
        ds.expect_column_values_to_be_between("weight_gram", min_value=1, max_value=None)
        ds.expect_column_values_to_be_between(
            "declared_value_vnd", min_value=0, max_value=None
        )


def validate_and_log(df: DataFrame, topic: str, event_type: str, run_end: str) -> dict:
    """Run Great Expectations on a Bronze window and log the result JSON to S3.

    Log-and-continue: never raises on validation failure — the result (including the
    per-expectation success/failure and unexpected counts) is written to the logs bucket
    so DQ trends can be inspected, and the caller proceeds to quarantine invalid rows.
    """
    # Imported lazily so the module imports even where great_expectations is absent
    # (e.g. the pure-python unit test environment).
    from great_expectations.dataset import SparkDFDataset

    ds = SparkDFDataset(df)
    _expectations(ds, topic, set(df.columns))
    result = ds.validate(result_format="SUMMARY").to_json_dict()

    bucket = os.environ.get("LOGS_BUCKET")
    if bucket:
        run_tag = run_end.replace(":", "").replace("+", "_")
        key = f"dq/{event_type}/{run_tag}/validation.json"
        boto3.client("s3").put_object(
            Bucket=bucket, Key=key, Body=json.dumps(result).encode("utf-8")
        )
        logger.info(
            "DQ %s: success=%s -> s3://%s/%s",
            event_type,
            result.get("success"),
            bucket,
            key,
        )
    else:
        logger.warning("LOGS_BUCKET unset; skipping DQ result upload for %s", event_type)

    return result
