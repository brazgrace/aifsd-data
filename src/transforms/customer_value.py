"""Task 2: tertile customer value from total sales, join segment to line items."""

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def add_value_segment(df):
    agg = df.groupBy("Customer_Id").agg(
        F.sum("Sales").alias("customer_total_sales"),
        F.sum("Profit").alias("customer_total_profit"),
    )
    w = Window.orderBy(F.col("customer_total_sales").asc())
    agg = (
        agg.withColumn("_tier", F.ntile(3).over(w))
        .withColumn(
            "customer_value_segment",
            F.when(F.col("_tier") == 1, F.lit("low_value"))
            .when(F.col("_tier") == 2, F.lit("medium_value"))
            .otherwise(F.lit("high_value")),
        )
        .drop("_tier")
    )
    return df.join(agg, "Customer_Id", "left")
