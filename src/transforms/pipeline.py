from src.transforms.cumulative_discount import add_cumulative_discount
from src.transforms.customer_value import add_value_segment
from src.transforms.dynamic_shipping import add_dynamic_shipping
from src.transforms.time_based import add_shipping_speed


def apply_all(df):
    df = add_shipping_speed(df)
    df = add_value_segment(df)
    df = add_cumulative_discount(df)
    df = add_dynamic_shipping(df)
    return df
