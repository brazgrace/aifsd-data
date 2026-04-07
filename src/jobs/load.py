import sqlite3

import pandas as pd


def write_staging_to_sqlite(staging_path: str, db_path: str, table_name: str) -> None:
    # Avoid Spark toPandas(): PySpark 3.3 imports distutils (removed in Python 3.12).
    pandas_df = pd.read_parquet(staging_path)
    con = sqlite3.connect(db_path)
    pandas_df.to_sql(table_name, con, if_exists="replace")
    con.close()


def load(spark, staging_path, db_path, table_name):
    write_staging_to_sqlite(staging_path, db_path, table_name)
    return spark.read.parquet(staging_path)
