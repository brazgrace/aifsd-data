"""Task 1: bucket Aging into shipping speed labels (fixed cutpoints)."""

from pyspark.sql import functions as F


def add_shipping_speed(df):
    return df.withColumn(
        "shipping_speed_category",
        F.when(F.col("Aging") <= 2, F.lit("express"))
        .when(F.col("Aging") <= 6, F.lit("standard"))
        .otherwise(F.lit("delayed")),
    )
