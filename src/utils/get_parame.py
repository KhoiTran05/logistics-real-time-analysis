from __future__ import annotations

import argparse
import json

from src.utils.logger import setup_logger

logger = setup_logger(__name__)


class ParsParam:
    def __call__(self):
        args, _ = self._parse_args()
        params = {
            "source": self._load_json_arg(
                args.source, "--source", allow_non_object=True
            )
        }
        
        self._validate_params(params)
        return params   

    def _validate_params(self, params):
        logger.info("Validating parameters...")
        if not isinstance(params.get("source"), list) or not params["source"]:
            logger.error("source must be a non-empty JSON array")
            raise ValueError("source must be a non-empty JSON array")
        logger.info("Parameter validation successful.")

    @staticmethod
    def _load_json_arg(value: str | None, name: str, allow_non_object: bool = False):
        if value in (None, ""):
            return {}
        try:
            data = json.loads(value)
        except json.JSONDecodeError as e:
            logger.error("Failed to parse JSON for %s: %s", name, e)
            if allow_non_object:
                return value
            raise ValueError(f"{name} must be valid JSON")
        if not allow_non_object and not isinstance(data, dict):
            raise ValueError(f"{name} must be a JSON object")
        return data

    @staticmethod
    def _parse_args():
        parser = argparse.ArgumentParser()
        parser.add_argument("--source", help="source data as JSON")
        return parser.parse_known_args()