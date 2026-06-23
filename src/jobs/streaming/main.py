from __future__ import annotations

import os

from src.jobs.streaming.bronze import bronze_batch_writer, start_bronze
from src.jobs.streaming.config import (
    TOPIC_FINANCIAL,
    TOPIC_SHIPMENT,
    TOPIC_TRACKING,
)
from src.jobs.streaming.dims import load_dims
from src.jobs.streaming.kpi import kpi_batch_writer, start_kpis
from src.jobs.streaming.session import build_spark
from src.jobs.streaming.source import parse, read_kafka
from src.jobs.streaming.transform import build_enriched
from src.jobs.streaming.ensure_tables import ensure_bronze_tables
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def main() -> None:
    bootstrap = os.environ["KAFKA_BOOTSTRAP"]
    warehouse = f"s3a://{os.environ['ICEBERG_BUCKET']}/warehouse/"
    ckpt_root = f"s3a://{os.environ['CHECKPOINTS_BUCKET']}/streaming"
    bronze_db = os.environ.get("BRONZE_DATABASE", "vdt_logistics_dev_bronze")
    dim_db = os.environ.get(
        "DIM_DATABASE", os.environ.get("GLUE_DATABASE", "vdt_logistics_dev_gold")
    )

    spark = build_spark(warehouse)
    ensure_bronze_tables(spark, bronze_db)
    dims = load_dims(spark, dim_db)

    for topic in (TOPIC_SHIPMENT, TOPIC_TRACKING, TOPIC_FINANCIAL):
        safe = topic.replace(".", "_")
        raw = read_kafka(spark, bootstrap, topic)
        parsed = parse(raw, topic)

        start_bronze(
            parsed,
            bronze_batch_writer(bronze_db, topic),
            f"{ckpt_root}/bronze/{safe}",
        )
        logger.info("Bronze writer started for %s", topic)

        enriched = build_enriched(parsed, topic, dims)
        start_kpis(
            enriched,
            kpi_batch_writer(topic),
            f"{ckpt_root}/kpis/{safe}"
        )
        logger.info("KPI stream started for %s", topic)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
