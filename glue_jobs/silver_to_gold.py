import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, count, max as _max

spark = SparkSession.builder.getOrCreate()

# Correct path for your actual Silver CSVs moved by Lambda
silver_path = "s3://swapnil-data-lake/silver/input/transactions/*/*/*/"

gold_path = "s3://swapnil-data-lake/gold/transactions_daily/"

# Read cleaned CSVs
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(silver_path)
)

# Normalize column names (Glue sometimes adds spaces)
df = df.toDF(*[c.strip().lower() for c in df.columns])

df_gold = (
    df.groupBy("account_id")
    .agg(
        _sum("amount").alias("total_amount"),
        count("*").alias("total_transactions"),
        _max("transaction_date").alias("last_transaction_date")
    )
)

# Write flat Parquet (required for Redshift COPY)
(
    df_gold
    .select("account_id", "total_amount", "total_transactions", "last_transaction_date")
    .coalesce(1)         # write ONE file only
    .write
    .mode("overwrite")
    .option("compression", "snappy")
    .parquet("s3://swapnil-data-lake/gold/transactions_daily/")
)
