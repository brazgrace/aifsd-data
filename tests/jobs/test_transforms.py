from src.transforms.pipeline import apply_all

from tests.schema_constants import ECOMMERCE_COLUMNS
from tests.spark_helpers import create_dataframe


def _sample_rows():
    return [
        (
            "2018-01-01",
            "10:00:00",
            1.0,
            1,
            "Female",
            "Web",
            "Member",
            "Auto & Accessories",
            "A",
            100.0,
            1.0,
            0.1,
            10.0,
            2.0,
            "High",
            "card",
        ),
        (
            "2018-01-02",
            "11:00:00",
            8.0,
            1,
            "Female",
            "Web",
            "Member",
            "Auto & Accessories",
            "B",
            50.0,
            2.0,
            0.2,
            5.0,
            1.0,
            "Critical",
            "card",
        ),
        (
            "2018-01-01",
            "09:00:00",
            3.0,
            2,
            "Male",
            "Web",
            "Guest",
            "Books",
            "C",
            20.0,
            1.0,
            0.05,
            2.0,
            1.5,
            "Low",
            "card",
        ),
    ]


def test_apply_all_adds_columns_and_logic(spark_session, tmp_path):
    df = create_dataframe(spark_session, _sample_rows(), ECOMMERCE_COLUMNS, tmp_path)
    out = apply_all(df)

    names = set(out.columns)
    assert "shipping_speed_category" in names
    assert "customer_value_segment" in names
    assert "cumulative_discount" in names
    assert "dynamic_shipping_cost" in names
    assert "profit_after_dynamic_shipping" in names

    row = out.filter(out.Customer_Id == 1).orderBy("order_ts").collect()
    assert row[0]["shipping_speed_category"] == "express"
    assert row[1]["shipping_speed_category"] == "delayed"
    assert row[0]["cumulative_discount"] == 0.1
    assert abs(row[1]["cumulative_discount"] - 0.3) < 1e-6
