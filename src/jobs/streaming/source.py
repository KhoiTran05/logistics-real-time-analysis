from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.jobs.streaming.config import MAX_OFFSETS
from src.jobs.streaming.ensure_tables import SCHEMA


def read_kafka(spark: SparkSession, bootstrap: str, topic: str) -> DataFrame:
    """Raw Kafka stream — value kept as string, kafka metadata preserved."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS[topic])
        .option("failOnDataLoss", "false")
        .option("kafka.fetch.min.bytes", "1")
        .load()
        .select(
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("value").cast("string").alias("raw_value"),
        )
    )


def parse(df: DataFrame, topic: str) -> DataFrame:
    """JSON → typed columns; normalize timestamps and lateness."""
    parsed = df.select(
        "kafka_partition",
        "kafka_offset",
        F.from_json(F.col("raw_value"), SCHEMA[topic]).alias("e"),
    ).select("kafka_partition", "kafka_offset", "e.*")

    return (
        parsed.withColumn("event_time", F.to_timestamp("event_time"))
        .withColumn("processing_time", F.to_timestamp("processing_time"))
        .withColumn("ingest_time", F.current_timestamp())
    )
