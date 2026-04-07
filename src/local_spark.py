"""Local SparkSession with loopback bind (avoids BindException when hostname/VPN is odd)."""

from pyspark.sql import SparkSession


def spark_session_builder(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )
