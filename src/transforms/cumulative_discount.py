"""Task 3: cumulative Discount along each customer's purchase timeline."""

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def add_cumulative_discount(df):
    # CSV inferSchema often makes Order_Date a date/timestamp and Time a timestamp;
    # casting both to string can yield "2018-04-22 00:00:00 2026-04-07 09:48:14".
    date_part = F.date_format(F.to_date(F.col("Order_Date")), "yyyy-MM-dd")
    time_part = F.regexp_extract(F.col("Time").cast("string"), r"(\d{2}:\d{2}:\d{2})", 1)
    time_part = F.when(F.length(time_part) == 0, F.lit("00:00:00")).otherwise(time_part)
    order_ts = F.to_timestamp(
        F.concat_ws(" ", date_part, time_part),
        "yyyy-MM-dd HH:mm:ss",
    )
    df = df.withColumn("order_ts", order_ts)
    # PySpark 3.3: use constants, not currentRow() (callable only in newer Spark).
    w = Window.partitionBy("Customer_Id").orderBy("order_ts").rowsBetween(
        Window.unboundedPreceding,
        Window.currentRow,
    )
    return df.withColumn("cumulative_discount", F.sum("Discount").over(w))
