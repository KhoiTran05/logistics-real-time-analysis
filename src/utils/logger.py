from __future__ import annotations

import logging
import os

DEFAULT_LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"


def setup_logger(
    name: str | None = None, level: str | None = None, fmt: str | None = None
):
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter(fmt or os.getenv("LOG_FORMAT", DEFAULT_LOG_FORMAT))
        )
        root_logger.addHandler(handler)

    resolved_level = (level or os.getenv("LOG_LEVEL", "INFO")).upper()
    root_logger.setLevel(resolved_level)

    return logging.getLogger(name) if name else root_logger