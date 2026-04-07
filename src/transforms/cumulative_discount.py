"""Task 3: cumulative Discount along each customer's purchase timeline."""

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def add_cumulative_discount(df):
    order_ts = F.to_timestamp(
        F.concat_ws(
            " ",
            F.col("Order_Date").cast("string"),
            F.col("Time").cast("string"),
        ),
        "yyyy-MM-dd HH:mm:ss",
    )
    df = df.withColumn("order_ts", order_ts)
    w = Window.partitionBy("Customer_Id").orderBy("order_ts").rowsBetween(
        Window.unboundedPreceding,
        Window.currentRow(),
    )
    return df.withColumn("cumulative_discount", F.sum("Discount").over(w))
