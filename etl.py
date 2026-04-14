import sys

try:
    import pyspark  # noqa: F401
except ModuleNotFoundError:
    sys.exit(
        "PySpark is not installed in this Python environment.\n"
        "From the ecom-etl-data directory:\n"
        "  python -m venv env && source env/bin/activate\n"
        "  pip install -e \".[dev]\"\n"
        "  python etl.py\n"
        "Or without activating:\n"
        "  ./env/bin/pip install -e \".[dev]\"\n"
        "  ./env/bin/python etl.py\n"
    )

from src.etl_config import load_etl_config
from src.jobs.extract import read_csv_file
from src.jobs.load import load
from src.jobs.transform import transform
from src.local_spark import spark_session_builder

if __name__ == "__main__":
    cfg = load_etl_config()
    spark = spark_session_builder(cfg.app_name)

    df = read_csv_file(spark, cfg.input_path)
    df = transform(spark, cfg.staging_path, df)
    df.show()
    print(df.count())
    df = load(spark, cfg.staging_path, cfg.db_path, cfg.table_name)

    spark.stop()
