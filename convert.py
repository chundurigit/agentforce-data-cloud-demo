import logging
import os
from pyspark.sql.functions import col, to_json, expr, date_format, monotonically_increasing_id, row_number
from pyspark.sql.types import StructType, ArrayType, MapType
from pyspark.sql import SparkSession, DataFrame, DataFrameWriter
from pyspark.sql.window import Window

# --- Setup current directory and input path ---
current_dir = os.getcwd()
input_path = os.path.join(current_dir, "data_files")

# --- Ensure data_files folder exists ---
if not os.path.exists(input_path):
    os.makedirs(input_path)
    print(f" Created missing folder: {input_path}")
    print("Please add at least one JSON file inside this folder and rerun.")
    exit(1)

print(f" Reading JSON files from: {input_path}")

# --- Check for .json files ---
json_files = [f for f in os.listdir(input_path) if f.endswith(".json")]
if not json_files:
    print(f" No JSON files found in {input_path}. Please add at least one JSON file and rerun.")
    exit(1)

# --- Initialize SparkSession ---
spark = (
    SparkSession.builder
    .appName("SparkLocal")
    .master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

# --- Read JSON input files ---
df = (
    spark.read
    .option("multiline", "true")
    .json(f"file://{input_path}/*.json")
)

print(f" Source row count: {df.count()}")


# --- Helper function to save data in desired format ---
def save_data(df_writer: DataFrameWriter, output_format: str, output_path: str):
    if output_format.lower() == "csv":
        df_writer = (
            df_writer
            .option("header", True)
            .option("quote", '"')
            .option("escape", '"')
            .option("escapeQuotes", True)
            .option("quoteAll", False)
            .option("sep", ",")
        )

    if output_format.lower() == "parquet":
        df_writer = df_writer.option("compression", "snappy")

    df_writer.save(output_path)


# --- Main function to process JSON ---
def process_json(json_df: DataFrame, output_format: str = "csv"):
    print(f" Generating {output_format} output...")

    cols = []
    opts = {
        "ignoreNullFields": "false",
        "timestampFormat": "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
        "dateFormat": "yyyy-MM-dd",
    }

    # Handle nested and timestamp fields
    for field in json_df.schema.fields:
        if isinstance(field.dataType, (StructType, ArrayType, MapType)):
            cols.append(to_json(col(field.name), opts).alias(field.name))
            continue

        if field.name in ("ts", "thread_ts"):
            cols.append(
                date_format(
                    expr("timestamp_micros(CAST((CAST(ts AS DOUBLE) * 1000000) AS BIGINT))"),
                    "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
                ).alias(field.name)
            )
            continue

        cols.append(col(field.name))

    # --- Step 1: Prepare base DataFrame ---
    modified_df = json_df.select(*cols).repartition(1)

    # --- Step 2: Add sequential primary key to all rows ---
    window = Window.orderBy(monotonically_increasing_id())
    numbered_df = modified_df.withColumn("primary_key", row_number().over(window))

    # Move primary_key column to start
    final_df = numbered_df.select(["primary_key"] + modified_df.columns)

    # --- Step 3: Write output ---
    output_path = f"{current_dir}/{output_format}_output"
    df_writer = final_df.write.mode("overwrite").format(output_format)
    save_data(df_writer=df_writer, output_format=output_format, output_path=output_path)

    print(f" {output_format.upper()} written to: {output_path}")


# --- Run for CSV, Parquet, JSON ---
process_json(json_df=df, output_format="csv")
process_json(json_df=df, output_format="parquet")
process_json(json_df=df, output_format="json")


# --- Verify CSV output ---
csv_output_path = f"{current_dir}/csv_output"
print(f" Reading generated CSV from: {csv_output_path}")

csv_df = (
    spark.read
    .option("inferSchema", True)
    .option("header", True)
    .option("multiline", True)
    .option("quote", '"')
    .option("escape", '"')
    .option("escapeQuotes", True)
    .option("sep", ",")
    .csv(f"file://{csv_output_path}/*.csv")
)

csv_df.printSchema()
print(f" Destination row count: {csv_df.count()}")
