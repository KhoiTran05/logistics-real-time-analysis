from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.jobs.streaming.config import CATALOG, TRIGGER_INTERVAL
from src.jobs.streaming.ensure_tables import TOPIC_EVENTS, bronze_columns


def bronze_batch_writer(database: str, topic: str):
    """foreachBatch fn: split the micro-batch by event_type into its typed table."""
    events = TOPIC_EVENTS[topic]

    def _write(batch_df: DataFrame, batch_id: int) -> None:
        batch_df.persist()
        try:
            for event_type in events:
                cols = bronze_columns(topic, event_type)
                sub = batch_df.filter(F.col("event_type") == event_type).select(*cols)
                sub.writeTo(f"{CATALOG}.{database}.{event_type}").append()
        finally:
            batch_df.unpersist()

    return _write


def start_bronze(parsed: DataFrame, writer, checkpoint: str):
    return (
        parsed.writeStream.outputMode("append")
        .option("checkpointLocation", checkpoint)
        .foreachBatch(writer)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )
