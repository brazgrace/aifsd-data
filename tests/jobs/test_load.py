import sqlite3

import pandas as pd
from src.jobs.load import load, write_staging_to_sqlite

from tests.schema_constants import ECOMMERCE_COLUMNS


def _staging_rows():
    return [
        (
            "2018-01-01",
            "10:00:00",
            1.0,
            1,
            "Female",
            "Web",
            "Member",
            "Auto & Accessories",
            "A",
            100.0,
            1.0,
            0.1,
            10.0,
            2.0,
            "High",
            "card",
        ),
    ]


def test_write_staging_to_sqlite_round_trip(tmp_path):
    staging_dir = tmp_path / "staging"
    db_path = tmp_path / "ecommerce.db"
    pdf = pd.DataFrame(_staging_rows(), columns=ECOMMERCE_COLUMNS)
    pdf.to_parquet(staging_dir, index=False)

    write_staging_to_sqlite(str(staging_dir), str(db_path), "ecommerce")

    with sqlite3.connect(db_path) as con:
        cur = con.execute("select count(*) from ecommerce")
        assert cur.fetchone()[0] == 1


def test_load_returns_staging_dataframe(spark_session, tmp_path):
    staging_dir = tmp_path / "staging"
    db_path = tmp_path / "ecommerce.db"
    pdf = pd.DataFrame(_staging_rows(), columns=ECOMMERCE_COLUMNS)
    pdf.to_parquet(staging_dir, index=False)

    df_out = load(spark_session, str(staging_dir), str(db_path), "ecommerce")

    assert df_out.count() == 1
    assert set(df_out.columns) >= set(ECOMMERCE_COLUMNS)
