import pytest

from src.local_spark import spark_session_builder


@pytest.fixture(scope="session")
def spark_session():
    spark = spark_session_builder("pytest_spark")
    yield spark
    spark.stop()
