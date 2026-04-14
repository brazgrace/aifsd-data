"""Shared ecommerce CSV / DataFrame schema for tests.

``SAMPLE_CSV_PATH`` points at the committed tiny dataset under ``data/``; the full
pipeline default input is ``data/raw/ecommerce_data.csv`` (see ``src/etl_config.py``).
"""

ECOMMERCE_COLUMNS = [
    "Order_Date",
    "Time",
    "Aging",
    "Customer_Id",
    "Gender",
    "Device_Type",
    "Customer_Login_type",
    "Product_Category",
    "Product",
    "Sales",
    "Quantity",
    "Discount",
    "Profit",
    "Shipping_Cost",
    "Order_Priority",
    "Payment_method",
]

SAMPLE_CSV_PATH = "data/sample.csv"
