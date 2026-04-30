from __future__ import annotations

import os


def get_hdfs_base_uri() -> str:
    hdfs_base_uri = os.getenv("HDFS_BASE_URI")

    if not hdfs_base_uri:
        raise ValueError("Variável de ambiente HDFS_BASE_URI não definida.")

    return hdfs_base_uri.rstrip("/")


def default_spark_local_dir() -> str:
    configured_dir = os.getenv("SPARK_LOCAL_DIRS") or os.getenv("SPARK_LOCAL_DIR")
    if configured_dir:
        return configured_dir.split(",")[0]

    candidates = [
        "/srv/spark-local",
        os.path.expanduser("~/spark-local"),
        "/tmp/spark-local",
    ]

    for candidate in candidates:
        try:
            os.makedirs(candidate, exist_ok=True)
        except OSError:
            continue

        if os.access(candidate, os.W_OK):
            return candidate

    return "/tmp"


def is_truthy(value: str | None) -> bool:
    if value is None:
        return False

    return value.strip().lower() in {"1", "true", "t", "yes", "y", "sim", "s"}
