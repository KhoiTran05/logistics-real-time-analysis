from __future__ import annotations

import os

from src.utils.ensure_tables import (
    CATALOG,
    TOPIC_FINANCIAL,
    TOPIC_SHIPMENT,
    TOPIC_TRACKING,
)

# Re-export so the rest of the streaming package has a single config import.
__all__ = [
    "CATALOG",
    "TOPIC_SHIPMENT",
    "TOPIC_TRACKING",
    "TOPIC_FINANCIAL",
    "WATERMARK",
    "MAX_OFFSETS",
    "TRIGGER_INTERVAL",
    "KPI_WINDOW_DURATION",
    "KPI_WINDOW_SLIDING_DURATION",
    "CLICKHOUSE_JDBC_URL",
    "CLICKHOUSE_USER",
    "CLICKHOUSE_PASSWORD",
    "CLICKHOUSE_DRIVER",
]

WATERMARK = {
    TOPIC_SHIPMENT: os.environ.get("WM_SHIPMENT", "60 seconds"),
    TOPIC_TRACKING: os.environ.get("WM_TRACKING", "60 seconds"),
    TOPIC_FINANCIAL: os.environ.get("WM_FINANCIAL", "60 seconds"),
}

MAX_OFFSETS = {
    TOPIC_SHIPMENT: 20000,
    TOPIC_TRACKING: 80000,
    TOPIC_FINANCIAL: 8000,
}

TRIGGER_INTERVAL = os.environ.get("TRIGGER_INTERVAL", "10 seconds")
KPI_WINDOW_DURATION = os.environ.get("KPI_WINDOW_DURATION", "5 minutes")
KPI_WINDOW_SLIDING_DURATION = os.environ.get("KPI_WINDOW_SLIDING_DURATION", "1 minute")

CLICKHOUSE_JDBC_URL = (
    f"jdbc:clickhouse://{os.environ.get('CLICKHOUSE_HOST', 'localhost')}"
    f":{os.environ.get('CLICKHOUSE_PORT', '8123')}"
    f"/{os.environ.get('CLICKHOUSE_DB', 'logistics')}"
)
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "admin")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver"
