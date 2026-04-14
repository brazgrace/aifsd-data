"""Environment-driven paths and names for the CLI ETL entrypoint."""

from __future__ import annotations

import os
from dataclasses import dataclass

_ENV_PREFIX = "ECOM_ETL_"


def _env(key: str, default: str) -> str:
    return os.environ.get(f"{_ENV_PREFIX}{key}", default)


@dataclass(frozen=True)
class EtlConfig:
    app_name: str
    input_path: str
    staging_path: str
    db_path: str
    table_name: str


def load_etl_config() -> EtlConfig:
    """Load settings from ``ECOM_ETL_*`` variables with repo-relative defaults."""
    return EtlConfig(
        app_name=_env("APP_NAME", "ECommerce ETL Pipeline"),
        input_path=_env("INPUT_PATH", "data_source/raw/ecommerce_data.csv"),
        staging_path=_env("STAGING_PATH", "data_source/staging/ecommerce"),
        db_path=_env("DB_PATH", "data_source/processed/ecommerce.db"),
        table_name=_env("TABLE_NAME", "ecommerce"),
    )
