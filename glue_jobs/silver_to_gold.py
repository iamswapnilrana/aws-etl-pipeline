import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, count, max as _max

spark = SparkSession.builder.appName("silver_to_gold").getOrCreate()

silver_path = "s3://swapnil-data-lake/silver/transactions/"
gold_path = "s3://swapnil-data-lake/gold/transactions_daily/"

# Read Silver layer
df = spark.read.parquet(silver_path)

# Daily aggregation
df_gold = (
    df.groupBy("account_id")
      .agg(
           _sum("amount").alias("total_amount"),
           count("*").alias("total_transactions"),
           _max("transaction_date").alias("last_transaction_date")
      )
)

# Write to Gold
df_gold.write.mode("overwrite").parquet(gold_path)

print("SILVER → GOLD completed")