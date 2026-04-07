from src.transforms.pipeline import apply_all


def transform(spark, staging_path, df):
    df = apply_all(df)
    df.write.parquet(staging_path, mode="overwrite")
    return df
