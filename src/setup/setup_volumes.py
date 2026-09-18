# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Setup — Create Schema and Volume
# MAGIC This runs first in the pipeline to ensure the target schema and raw_data volume exist.

# COMMAND ----------
# Get parameters from DAB job
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------
# Create schema if not exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
print(f"Schema ready: {catalog}.{schema}")

# COMMAND ----------
# Create volume for raw data if not exists
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.raw_data")
print(f"Volume ready: /Volumes/{catalog}/{schema}/raw_data/")

# COMMAND ----------
# Verify volume is accessible
try:
    files = dbutils.fs.ls(f"/Volumes/{catalog}/{schema}/raw_data/")
    print(f" Volume contains {len(files)} file(s):")
    for f in files:
        print(f"   {f.name} ({f.size} bytes)")
except Exception as e:
    print(f"Volume exists but may be empty: {e}")
