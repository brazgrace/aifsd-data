# Agent notes (ecom-etl-data)

Use this file as a quick orientation when editing or reviewing this repo in an automated or assistant context.

## What this project is

A small **PySpark** ETL pipeline: CSV extract → Parquet staging (transforms in `src/transforms/`) → SQLite load via **pandas/pyarrow** (see `src/jobs/load.py`).

## Layout

| Path | Role |
|------|------|
| `etl.py` | CLI-style entrypoint; reads config from env (see below). |
| `src/etl_config.py` | `ECOM_ETL_*` environment variables and defaults. |
| `src/jobs/` | `extract`, `transform`, `load`. |
| `src/transforms/` | Business logic chained in `pipeline.apply_all`. |
| `src/local_spark.py` | `SparkSession` with driver bound to `127.0.0.1`. |
| `data/sample.csv` | Tiny committed dataset for tests (not the full Kaggle file). |
| `data_source/` | Default local I/O root for the full ETL run (typically gitignored under `raw/`, `staging/`, `processed/`). |

## Environment variables

All optional; defaults match the previous hard-coded paths.

| Variable | Default |
|----------|---------|
| `ECOM_ETL_APP_NAME` | `ECommerce ETL Pipeline` |
| `ECOM_ETL_INPUT_PATH` | `data_source/raw/ecommerce_data.csv` |
| `ECOM_ETL_STAGING_PATH` | `data_source/staging/ecommerce` |
| `ECOM_ETL_DB_PATH` | `data_source/processed/ecommerce.db` |
| `ECOM_ETL_TABLE_NAME` | `ecommerce` |

Example:

```bash
export ECOM_ETL_INPUT_PATH=data/sample.csv
python etl.py
```

## Install and run

From `ecom-etl-data/`:

```bash
python -m venv env
source env/bin/activate
pip install -e ".[dev]"
python etl.py
```

**Java:** PySpark **3.3.4** expects **Java 8 or 11** (`JAVA_HOME` accordingly). Newer PySpark needs Java 17+.

## Tests

```bash
pytest
```

PySpark **3.3** on **Python 3.12** has rough edges for `SparkSession.createDataFrame` from in-memory rows and for pandas 2.x (`iteritems`). Tests use **`tests/spark_helpers.create_dataframe`** (pandas → Parquet → `spark.read.parquet`) to stay stable.

## Lint and types (optional)

```bash
ruff check .
mypy
```

`mypy` expects the **`dev`** extra (`pandas-stubs` is listed there).

## Do not commit

Virtualenv (`env/`), `.env` secrets, large or raw datasets, Parquet/SQLite outputs, and default `data_source/{raw,staging,processed}/` contents (see `.gitignore`).
