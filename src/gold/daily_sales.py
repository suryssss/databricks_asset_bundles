# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Gold Layer — Daily Sales Aggregation
# MAGIC Joins silver_orders with silver_order_items to produce daily revenue metrics.

# COMMAND ----------
# Get parameters from DAB job
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------
from pyspark.sql.functions import (
    col, sum as _sum, countDistinct, round as _round, current_timestamp
)

# COMMAND ----------
# Read Silver tables
silver_orders = spark.table(f"{catalog}.{schema}.silver_orders")
silver_order_items = spark.table(f"{catalog}.{schema}.silver_order_items")

# COMMAND ----------
# Join orders with order items and aggregate by date
gold_daily_sales = (
    silver_orders
    .filter(col("order_date").isNotNull())
    .join(silver_order_items, "order_id", "inner")
    .groupBy("order_date")
    .agg(
        countDistinct("order_id").alias("total_orders"),
        _sum("quantity").alias("total_items_sold"),
        _round(_sum(col("quantity") * col("unit_price")), 2).alias("total_revenue")
    )
    .orderBy("order_date")
    .withColumn("gold_timestamp", current_timestamp())
)

# COMMAND ----------
# Write Gold table
gold_daily_sales.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.gold_daily_sales")
print(f"Gold daily_sales: {gold_daily_sales.count()} rows written")

# COMMAND ----------
# Preview
display(gold_daily_sales)
