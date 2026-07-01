"""
Render a batch SparkApplication manifest for SparkKubernetesOperator.
"""
from __future__ import annotations

import os
from typing import Any

import boto3
import yaml

TEMPLATE_KEY = "configs/spark/batch-app-template.yaml"
CONFIG_KEY = "configs/spark/app-config.yaml"

RUN_START_MACRO = (
    "{{ (macros.datetime.utcnow() - macros.timedelta(hours=2)).strftime('%Y-%m-%dT%H:%M:%S+00:00') "
    "if dag_run.run_type == 'manual' else data_interval_start }}"
)
RUN_END_MACRO = (
    "{{ macros.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S+00:00') "
    "if dag_run.run_type == 'manual' else data_interval_end }}"
)


class _SafeDict(dict):
    def __missing__(self, key: str) -> str:
        return "{" + key + "}"


def _render(value: Any, ctx: dict[str, Any]) -> Any:
    if isinstance(value, str):
        return value.format_map(_SafeDict(ctx))
    if isinstance(value, list):
        return [_render(v, ctx) for v in value]
    if isinstance(value, dict):
        return {k: _render(v, ctx) for k, v in value.items()}
    return value


def _deep_get(data: dict, *keys: str, default: Any = None) -> Any:
    cur = data
    for k in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
    return cur if cur is not None else default


def _s3_text(bucket: str, key: str) -> str:
    body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
    return body.decode("utf-8")


def _job(config: dict, job_name: str) -> dict:
    for job in config.get("jobs", []):
        if job.get("name") == job_name:
            return job
    raise ValueError(f"job '{job_name}' not found in {CONFIG_KEY}")


def render(job_name: str) -> str:
    """Return a SparkApplication YAML string (with `{{ data_interval_* }}` macros intact)."""
    artifacts = os.environ["S3_ARTIFACTS_BUCKET"]
    template = yaml.safe_load(_s3_text(artifacts, TEMPLATE_KEY))
    config = yaml.safe_load(_s3_text(artifacts, CONFIG_KEY))
    job = _job(config, job_name)

    ctx = {
        "SPARK_APP_NAME": job.get("application_name"),
        "SPARK_IMAGE": os.environ["SPARK_IMAGE"],
        "MAIN_APPLICATION_FILE": job.get("application_file"),
        "DRIVER_CORES": _deep_get(job, "driver", "cores", default=1),
        "DRIVER_MEMORY": _deep_get(job, "driver", "memory", default="1g"),
        "DRIVER_SERVICE_ACCOUNT": "spark",
        "EXECUTOR_INSTANCES": _deep_get(job, "executor", "instances", default=1),
        "EXECUTOR_CORES": _deep_get(job, "executor", "cores", default=1),
        "EXECUTOR_MEMORY": _deep_get(job, "executor", "memory", default="1g"),
        "ICEBERG_BUCKET": os.environ["S3_ICEBERG_BUCKET"],
        "CHECKPOINTS_BUCKET": os.environ["S3_CHECKPOINTS_BUCKET"],
        "LOGS_BUCKET": os.environ["S3_LOGS_BUCKET"],
    }

    app = _render(template, ctx)
    app["spec"]["arguments"] = [
        a.replace("{RUN_START}", RUN_START_MACRO).replace("{RUN_END}", RUN_END_MACRO)
        for a in job.get("arguments", [])
    ]

    driver_override = {k: v for k, v in job.get("driver", {}).items() if k != "env"}
    app["spec"]["driver"].update(driver_override)
    app["spec"]["executor"].update(job.get("executor", {}))
    app["spec"]["driver"]["env"] = app["spec"]["driver"].get("env", []) + _deep_get(
        job, "driver", "env", default=[]
    )
    if job.get("py_files"):
        app["spec"].setdefault("deps", {})["pyFiles"] = job["py_files"]

    return yaml.safe_dump(app, sort_keys=False)
