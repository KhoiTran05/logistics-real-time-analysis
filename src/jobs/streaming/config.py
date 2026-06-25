from __future__ import annotations

import os

from src.jobs.streaming.ensure_tables import (
    CATALOG,
    TOPIC_FINANCIAL,
    TOPIC_SHIPMENT,
    TOPIC_TRACKING,
)
from src.jobs.streaming.shipment_state import (  
    HUB_DWELL_EXPRESS_S,
    HUB_DWELL_STANDARD_S,
    LOST_TIMEOUT_S,
    STUCK_TIMEOUT_S,
)


__all__ = [
    "CATALOG",
    "TOPIC_SHIPMENT",
    "TOPIC_TRACKING",
    "TOPIC_FINANCIAL",
    "WATERMARK",
    "WM_UNIFIED",
    "STUCK_TIMEOUT_S",
    "LOST_TIMEOUT_S",
    "HUB_DWELL_EXPRESS_S",
    "HUB_DWELL_STANDARD_S",
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

WM_UNIFIED = os.environ.get("WM_UNIFIED", "60 seconds")

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
