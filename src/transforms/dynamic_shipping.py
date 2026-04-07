"""Task 4: heuristic dynamic shipping vs recorded Shipping_Cost; profit adjustment."""

from pyspark.sql import functions as F


def add_dynamic_shipping(df):
    # Base from quantity + sales share; scaled by priority; mild category bump.
    base = F.col("Quantity") * 0.75 + F.col("Sales") * 0.018
    priority = (
        F.when(F.col("Order_Priority") == "Critical", F.lit(1.35))
        .when(F.col("Order_Priority") == "High", F.lit(1.15))
        .when(F.col("Order_Priority") == "Medium", F.lit(1.0))
        .when(F.col("Order_Priority") == "Low", F.lit(0.92))
        .otherwise(F.lit(1.0))
    )
    cat = F.when(
        F.col("Product_Category") == "Auto & Accessories",
        F.lit(1.05),
    ).otherwise(F.lit(1.0))
    dyn = base * priority * cat
    df = df.withColumn("dynamic_shipping_cost", F.round(dyn, 4))
    # Profit if we replaced recorded shipping with dynamic estimate.
    return df.withColumn(
        "profit_after_dynamic_shipping",
        F.round(
            F.col("Profit") - (F.col("dynamic_shipping_cost") - F.col("Shipping_Cost")),
            4,
        ),
    )
