# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Gold Layer — Customer Sales Aggregation
# MAGIC Joins silver_orders, silver_order_items, and silver_customers to produce per-customer spending metrics.

# COMMAND ----------
# Get parameters from DAB job
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------
from pyspark.sql.functions import (
    col, sum as _sum, countDistinct, round as _round, current_timestamp,
    min as _min, max as _max
)

# COMMAND ----------
# Read Silver tables
silver_orders = spark.table(f"{catalog}.{schema}.silver_orders")
silver_order_items = spark.table(f"{catalog}.{schema}.silver_order_items")
silver_customers = spark.table(f"{catalog}.{schema}.silver_customers")

# COMMAND ----------
# Join orders → order_items → customers and aggregate by customer
gold_customer_sales = (
    silver_orders
    .join(silver_order_items, "order_id", "inner")
    .join(silver_customers, "customer_id", "inner")
    .groupBy("customer_id", "customer_name", "email", "city", "country")
    .agg(
        countDistinct("order_id").alias("total_orders"),
        _sum("quantity").alias("total_items_bought"),
        _round(_sum(col("quantity") * col("unit_price")), 2).alias("total_spent"),
        _min("order_date").alias("first_order_date"),
        _max("order_date").alias("last_order_date")
    )
    .orderBy(col("total_spent").desc())
    .withColumn("gold_timestamp", current_timestamp())
)

# COMMAND ----------
# Write Gold table
gold_customer_sales.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.gold_customer_sales")
print(f"Gold customer_sales: {gold_customer_sales.count()} rows written")

# COMMAND ----------
# Preview
display(gold_customer_sales)
