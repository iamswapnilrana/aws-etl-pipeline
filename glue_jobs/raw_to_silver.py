import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

spark = SparkSession.builder.appName("raw_to_silver").getOrCreate()

input_path = "s3://swapnil-data-lake/silver/input/"
output_path = "s3://swapnil-data-lake/silver/transactions/"

df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(input_path)

# Clean column names (Glue sometimes trims whitespace)
df = df.toDF(*[c.strip().lower() for c in df.columns])

# Now amount exists as "amount"
df_clean = (
    df.dropna(subset=["amount"])
    .withColumn("amount", col("amount").cast("double"))
    .withColumn("transaction_date", col("transaction_date").cast("date"))
    .withColumn("ingestion_time", current_timestamp())
)

df_clean.write.mode("append").partitionBy("transaction_date").parquet(output_path)

print("RAW → SILVER completed")
