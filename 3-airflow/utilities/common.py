"""Shared helpers for every pipeline in this Airflow project."""

import os
from pathlib import Path

import yaml

DAGS_DIR = Path(__file__).resolve().parent.parent / "dags"

RUNTIMES = ("development", "test", "production")


def get_runtime():
    """Return the runtime this Airflow instance is serving.

    Set by `APOLITICAL_RUNTIME` in the environment. Defaults to `development`, so a
    local instance never behaves as if it were production.
    """
    runtime = os.environ.get("APOLITICAL_RUNTIME", "development")
    if runtime not in RUNTIMES:
        raise ValueError(f"APOLITICAL_RUNTIME must be one of {RUNTIMES}, got {runtime!r}")
    return runtime


def get_dag_configs(pipeline, runtime):
    """Load `configs.yaml` for a pipeline, with the runtime's overrides applied.

    The file's `common` block applies everywhere; a block named after the runtime
    overrides it key by key.
    """
    config_file = DAGS_DIR / pipeline / "configs.yaml"
    configs = yaml.safe_load(config_file.read_text())
    common = dict(configs.get("common", {}))
    common.update(configs.get(runtime, {}) or {})
    return {**configs, "common": common}
