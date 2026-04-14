import sqlite3

import pandas as pd
from pyspark.sql import SparkSession


def write_staging_to_sqlite(staging_path: str, db_path: str, table_name: str) -> None:
    # Avoid Spark toPandas(): PySpark 3.3 imports distutils (removed in Python 3.12).
    pandas_df = pd.read_parquet(staging_path)
    with sqlite3.connect(db_path) as con:
        pandas_df.to_sql(table_name, con, if_exists="replace")


def load(
    spark: SparkSession, staging_path: str, db_path: str, table_name: str
):
    """Persist staging Parquet to SQLite via pandas, then return the staging DataFrame."""
    write_staging_to_sqlite(staging_path, db_path, table_name)
    return spark.read.parquet(staging_path)
