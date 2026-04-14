from src.jobs.extract import read_csv_file

from tests.schema_constants import ECOMMERCE_COLUMNS, SAMPLE_CSV_PATH


def test_read_csv_file(spark_session):
    df = read_csv_file(spark_session, SAMPLE_CSV_PATH)

    assert df.count() == 3
    assert df.columns == ECOMMERCE_COLUMNS
