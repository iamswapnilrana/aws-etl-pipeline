import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

spark = SparkSession.builder.appName("raw_to_silver").getOrCreate()

input_path = "s3://swapnil-data-lake/silver/input/"
output_path = "s3://swapnil-data-lake/silver/transactions/"

# Read all new files in Silver input
df = spark.read.option("header", True).csv(input_path)

# Basic cleaning
df_clean = (
    df.dropna(subset=["amount"])  # remove null amounts
      .withColumn("amount", col("amount").cast("double"))
      .withColumn("transaction_date", col("transaction_date").cast("date"))
      .withColumn("ingestion_time", current_timestamp())
)

# Write as partitioned Parquet
df_clean.write.mode("append").partitionBy("transaction_date").parquet(output_path)

print("RAW → SILVER completed")
