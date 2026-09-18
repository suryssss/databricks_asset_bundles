# Databricks notebook source

# COMMAND ----------
# Get parameters from DAB job
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------
# Ensure schema exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------
from pyspark.sql.functions import current_timestamp

data_path = f"/Volumes/{catalog}/{schema}/raw_data/ord_data.csv"
df = spark.read.csv(data_path, header=True, inferSchema=True)
df = df.withColumn("ingestion_timestamp", current_timestamp())
df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.bronze_orders")
print(f"Bronze orders: {df.count()} rows written")
