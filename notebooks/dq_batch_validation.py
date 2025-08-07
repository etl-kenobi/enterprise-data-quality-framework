# Databricks notebook source
# MAGIC %md
# MAGIC ## Batch Data Quality Validation - PySpark

# COMMAND ----------
from pyspark.sql import SparkSession
from dq_checks import DQValidator

spark = SparkSession.builder.appName("DQFramework").getOrCreate()

# Load datasets
customers = spark.read.option("header", True).csv("/dbfs/FileStore/datasets/customers.csv")
transactions = spark.read.option("header", True).csv("/dbfs/FileStore/datasets/transactions.csv")

# Validate customers
dq = DQValidator(customers)
nulls_df = dq.null_check(["customer_id", "name", "email"])
duplicates_df = dq.duplicate_check(["customer_id"])
nulls_df.show()
duplicates_df.show()

# COMMAND ----------
# Validate transactions
dq_txn = DQValidator(transactions)
out_of_range_df = dq_txn.range_check("quantity", 1, 5)
missing_customers_df = dq.referential_integrity_check(transactions, "customer_id")
out_of_range_df.show()
missing_customers_df.show()
