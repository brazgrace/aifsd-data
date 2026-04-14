"""Spark test helpers.

PySpark 3.3 on Python 3.12: in-memory createDataFrame(list) fails to pickle; createDataFrame(pandas)
expects pandas 1.x (iteritems). Build frames via Parquet round-trip instead.
"""

from collections.abc import Sequence
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd
from pyspark.sql import SparkSession


def create_dataframe(
    spark: SparkSession,
    rows: Sequence[Sequence[Any]],
    columns: list[str],
    tmp_path: Path,
):
    out = tmp_path / f"spark_df_{uuid4().hex}"
    pd.DataFrame(rows, columns=columns).to_parquet(out, index=False)
    return spark.read.parquet(str(out))
