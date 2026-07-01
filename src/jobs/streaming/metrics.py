from __future__ import annotations

import json
import math
import os
import time
import urllib.parse
import urllib.request

from pyspark.sql.streaming.listener import StreamingQueryListener

from src.utils.logger import setup_logger

logger = setup_logger(__name__)

_CH_URL = (
    f"http://{os.environ.get('CLICKHOUSE_HOST', 'localhost')}"
    f":{os.environ.get('CLICKHOUSE_PORT', '8123')}/"
)
_CH_DB = os.environ.get("CLICKHOUSE_DB", "logistics")
_CH_USER = os.environ.get("CLICKHOUSE_USER", "admin")
_CH_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")
_INSERT_QUERY = f"INSERT INTO {_CH_DB}.streaming_query_progress FORMAT JSONEachRow"


def _f(v) -> float:
    """Coerce to a finite float (progress rates are NaN/inf on empty batches)."""
    v = float(v) if v is not None else 0.0
    return v if math.isfinite(v) else 0.0


def _insert(row: dict) -> None:
    req = urllib.request.Request(
        f"{_CH_URL}?query={urllib.parse.quote(_INSERT_QUERY)}",
        data=json.dumps(row).encode("utf-8"),
        headers={"X-ClickHouse-User": _CH_USER, "X-ClickHouse-Key": _CH_PASSWORD},
        method="POST",
    )
    urllib.request.urlopen(req, timeout=5).read()


def _row(p) -> dict:
    """Flatten a StreamingQueryProgress to the streaming_query_progress schema.

    dropped-late comes from the dedup stateOperator (kpi_* streams) or the
    applyInPandasWithState operator (stateful stream); summed across operators.
    """
    dur = getattr(p, "durationMs", None) or {}
    ops = getattr(p, "stateOperators", None) or []
    return {
        "event_ts": int(time.time()),  # listener wall-clock
        "query_name": getattr(p, "name", None) or "unnamed",
        "batch_id": int(getattr(p, "batchId", 0) or 0),
        "num_input_rows": int(getattr(p, "numInputRows", 0) or 0),
        "input_rows_per_second": _f(getattr(p, "inputRowsPerSecond", 0.0)),
        "processed_rows_per_second": _f(getattr(p, "processedRowsPerSecond", 0.0)),
        "batch_duration_ms": int(dur.get("triggerExecution", 0) or 0),
        "add_batch_ms": int(dur.get("addBatch", 0) or 0),
        "state_num_rows_total": sum(int(o.numRowsTotal or 0) for o in ops),
        "state_memory_bytes": sum(int(o.memoryUsedBytes or 0) for o in ops),
        "num_dropped_late_rows": sum(int(o.numRowsDroppedByWatermark or 0) for o in ops),
    }


class ProgressListener(StreamingQueryListener):
    """Persist per-batch StreamingQueryProgress to ClickHouse for the config sweep."""

    def onQueryStarted(self, event) -> None:
        logger.info("stream started name=%s id=%s", event.name, event.id)

    def onQueryProgress(self, event) -> None:
        try:
            _insert(_row(event.progress))
        except Exception as exc: 
            logger.warning("progress insert failed: %s", exc)

    def onQueryIdle(self, event) -> None:
        pass

    def onQueryTerminated(self, event) -> None:
        logger.info("stream terminated id=%s", event.id)
