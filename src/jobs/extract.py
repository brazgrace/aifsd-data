def read_csv_file(spark, input_path):
    return (
        spark.read.format("csv")
        .options(header="true", inferSchema="true")
        .load(input_path)
    )
