import sys

try:
    import pyspark  # noqa: F401
except ModuleNotFoundError:
    sys.exit(
        "PySpark is not installed in this Python environment.\n"
        "From the ecom-etl-data directory:\n"
        "  source env/bin/activate && pip install -r requirements.txt\n"
        "  python etl.py\n"
        "Or without activating:\n"
        "  ./env/bin/python etl.py\n"
    )

from src.jobs.extract import read_csv_file
from src.jobs.load import load
from src.jobs.transform import transform
from src.local_spark import spark_session_builder

APP_NAME = 'ECommerce ETL Pipeline'
input_path = 'data_source/raw/ecommerce_data.csv'
staging_path = 'data_source/staging/ecommerce'
db_path = 'data_source/processed/ecommerce.db'
table_name = 'ecommerce'

if __name__ == '__main__':
    spark = spark_session_builder(APP_NAME)

    # Extract raw data
    df = read_csv_file(spark, input_path)

    # Transform data and store in staging
    df = transform(spark, staging_path, df)
    df.show()

    print(df.count())

    # Load data to processed database
    df = load(spark, staging_path, db_path, table_name)

    spark.stop()
