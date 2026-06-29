import yaml
import argparse
from pathlib import Path
from typing import Any

def _get_file_path(file_path: str) -> Path:
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File {file_path} not found.")
    return path

def _deep_get(data: dict[str, Any], *keys: str, default: Any = None) -> Any:
    current = data
    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
    return current if current is not None else default

def _load_spark_job_config(file_path: str, job_name: str) -> dict[str, Any]:
    path = _get_file_path(file_path)

    with path.open("r", encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}

    jobs = config.get("jobs")
    for job in jobs:
        if job.get("name") == job_name:
            return job

    available_jobs = ",".join(job.get("name", "<unknown>") for job in jobs)
    raise ValueError(f"Job '{job_name}' not found in {file_path}. Available jobs: {available_jobs}")

class SafeFormatDict(dict[str, Any]):
    def __missing__(self, key: str) -> str:
        return "{" + key + "}"

def _render_value(value: Any, context: dict[str, Any]) -> Any:
    if isinstance(value, str):
        return value.format_map(SafeFormatDict(context))
    if isinstance(value, list):
        return [_render_value(item, context) for item in value]
    if isinstance(value, dict):
        return {key: _render_value(item, context) for key, item in value.items()}
    return value

def render_application(
    app_template_path: str,
    app_config_path: str,
    job_name: str,
    target_path: str,
    spark_image: str,
    iceberg_bucket: str,
    checkpoints_bucket: str,
    logs_bucket: str | None = None,
):
    template_path = _get_file_path(app_template_path)
    with template_path.open("r", encoding="utf-8") as f:
        template = yaml.safe_load(f)

    job_config = _load_spark_job_config(app_config_path, job_name)

    context = {
        "SPARK_APP_NAME": job_config.get("application_name"),
        "SPARK_IMAGE": spark_image,
        "MAIN_APPLICATION_FILE": job_config.get("application_file"),
        "DRIVER_CORES": _deep_get(job_config, "driver", "cores", default=1),
        "DRIVER_MEMORY": _deep_get(job_config, "driver", "memory", "1g"),
        "DRIVER_SERVICE_ACCOUNT": "spark",
        "EXECUTOR_INSTANCES": _deep_get(job_config, "executor", "instances", default=1),
        "EXECUTOR_CORES": _deep_get(job_config, "executor", "cores", default=1),
        "EXECUTOR_MEMORY": _deep_get(job_config, "executor", "memory", default="1g"),
        "ICEBERG_BUCKET": iceberg_bucket,
        "CHECKPOINTS_BUCKET": checkpoints_bucket,
    }
    if logs_bucket:
        context["LOGS_BUCKET"] = logs_bucket


    application = _render_value(template, context)
    application["spec"]["arguments"] = job_config.get("arguments", [])


    driver_override = {k: v for k, v in job_config.get("driver", {}).items() if k != "env"}
    application["spec"]["driver"].update(driver_override)
    application["spec"]["executor"].update(job_config.get("executor", {}))

    template_env = application["spec"]["driver"].get("env", [])
    job_env = _deep_get(job_config, "driver", "env", default=[])
    application["spec"]["driver"]["env"] = template_env + job_env

    py_files = job_config.get("py_files")
    if py_files:
        application["spec"]["deps"].update({"pyFiles": py_files})

    with Path(target_path).open("w") as f:
        yaml.safe_dump(application, f, sort_keys=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--app_template_path")
    parser.add_argument("--app_config_path")
    parser.add_argument("--job_name")
    parser.add_argument("--target_path")
    parser.add_argument("--spark_image")
    parser.add_argument("--iceberg_bucket")
    parser.add_argument("--checkpoints_bucket")
    parser.add_argument("--logs_bucket")

    args = parser.parse_args()

    render_application(
        args.app_template_path,
        args.app_config_path,
        args.job_name,
        args.target_path,
        args.spark_image,
        args.iceberg_bucket,
        args.checkpoints_bucket,
        args.logs_bucket,
    )
        

    