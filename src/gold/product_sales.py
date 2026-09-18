# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Gold Layer — Product Sales Aggregation
# MAGIC Joins silver_order_items with silver_products to produce per-product sales metrics.

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
silver_order_items = spark.table(f"{catalog}.{schema}.silver_order_items")
silver_products = spark.table(f"{catalog}.{schema}.silver_products")

# COMMAND ----------
# Join order items with products and aggregate by product
gold_product_sales = (
    silver_order_items
    .join(silver_products, "product_id", "inner")
    .groupBy("product_id", "product_name", "category")
    .agg(
        countDistinct("order_id").alias("total_orders"),
        _sum("quantity").alias("total_quantity_sold"),
        _round(_sum(col("quantity") * col("unit_price")), 2).alias("total_revenue")
    )
    .orderBy(col("total_revenue").desc())
    .withColumn("gold_timestamp", current_timestamp())
)

# COMMAND ----------
# Write Gold table
gold_product_sales.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.gold_product_sales")
print(f" Gold product_sales: {gold_product_sales.count()} rows written")

# COMMAND ----------
# Preview
display(gold_product_sales)
