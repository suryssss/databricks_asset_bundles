# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Silver Layer — Clean & Transform
# MAGIC Reads all 4 Bronze tables, applies data quality rules, and writes cleaned Silver tables.
# MAGIC Bad records go to `silver_quarantine` for auditing.

# COMMAND ----------
# Get parameters from DAB job
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------
# Configure Spark: allow graceful null handling for dirty raw data
spark.conf.set("spark.sql.ansi.enabled", "false")

# COMMAND ----------
from pyspark.sql.functions import (
    col, trim, lower, upper, initcap, regexp_replace,
    to_date, coalesce, when, lit, current_timestamp,
    concat_ws, expr
)
from pyspark.sql.types import DoubleType, IntegerType

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Silver Customers

# COMMAND ----------
# Read Bronze
bronze_customers = spark.table(f"{catalog}.{schema}.bronze_customers")

silver_customers = (
    bronze_customers
    # 1. Deduplicate on customer_id
    .dropDuplicates(["customer_id"])
    # 2. Drop rows where customer_id is null
    .filter(col("customer_id").isNotNull())
    # 3. Trim whitespace from string columns
    .withColumn("customer_name", trim(col("customer_name")))
    .withColumn("email", trim(lower(col("email"))))
    .withColumn("city", trim(initcap(col("city"))))
    .withColumn("country", trim(col("country")))
    # 4. Standardize country names
    .withColumn("country", 
        when(lower(col("country")).isin("usa", "u.s.a.", "united states"), "USA")
        .when(lower(col("country")).isin("uk", "united kingdom"), "UK")
        .when(lower(col("country")).isin("in", "india"), "India")
        .otherwise(initcap(col("country")))
    )
    # 5. Clean phone — replace "unknown" with null
    .withColumn("phone", when(lower(col("phone")) == "unknown", None).otherwise(col("phone")))
    # 6. Clean city — replace "N/A" with null
    .withColumn("city", when(lower(col("city")) == "n/a", None).otherwise(col("city")))
    # 7. Clean age — set negatives and impossibles to null
    .withColumn("age", 
        when((col("age") < 0) | (col("age") > 120), None).otherwise(col("age"))
    )
    # 8. Parse signup_date — handle multiple formats safely with try_to_date
    .withColumn("signup_date",
        coalesce(
            expr("try_to_date(signup_date, 'yyyy-MM-dd')"),
            expr("try_to_date(signup_date, 'M/d/yyyy')"),
            expr("try_to_date(signup_date, 'd-MMM-yyyy')"),
            expr("try_to_date(signup_date, 'dd-MMM-yyyy')")
        )
    )
    # 9. Standardize customer_name to Title Case
    .withColumn("customer_name", initcap(lower(col("customer_name"))))
    # 10. Add processing timestamp
    .withColumn("silver_timestamp", current_timestamp())
)

# Write Silver customers
silver_customers.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.silver_customers")
print(f"✅ Silver customers: {silver_customers.count()} rows (from {bronze_customers.count()} bronze)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Silver Products

# COMMAND ----------
bronze_products = spark.table(f"{catalog}.{schema}.bronze_products")

silver_products = (
    bronze_products
    # 1. Deduplicate on product_id
    .dropDuplicates(["product_id"])
    # 2. Drop rows where product_id is null
    .filter(col("product_id").isNotNull())
    # 3. Trim whitespace
    .withColumn("product_name", trim(initcap(lower(col("product_name")))))
    .withColumn("category", trim(initcap(lower(col("category")))))
    # 4. Clean price — remove "$", handle "N/A", cast to double
    .withColumn("price", regexp_replace(col("price"), "[$\\s]", ""))
    .withColumn("price", 
        when(lower(col("price")).isin("n/a", ""), None).otherwise(col("price"))
    )
    .withColumn("price", expr("try_cast(price as double)"))
    # 5. Filter out null/zero/negative prices
    .filter((col("price").isNotNull()) & (col("price") > 0))
    # 6. Clean stock_quantity — handle "in stock", negatives
    .withColumn("stock_quantity",
        when(lower(col("stock_quantity")) == "in stock", None)
        .otherwise(col("stock_quantity"))
    )
    .withColumn("stock_quantity", expr("try_cast(stock_quantity as int)"))
    .withColumn("stock_quantity",
        when(col("stock_quantity") < 0, lit(0)).otherwise(col("stock_quantity"))
    )
    # 7. Standardize category names
    .withColumn("category",
        when(lower(col("category")).contains("electronic"), "Electronics")
        .when(lower(col("category")).contains("cloth"), "Clothing")
        .when(lower(col("category")).contains("home"), "Home & Kitchen")
        .when(lower(col("category")).contains("kitchen"), "Home & Kitchen")
        .when(lower(col("category")).contains("book"), "Books")
        .when(lower(col("category")).contains("toy"), "Toys")
        .when(lower(col("category")).contains("sport"), "Sports")
        .when(lower(col("category")).contains("beauty"), "Beauty")
        .when(lower(col("category")).contains("grocery"), "Grocery")
        .otherwise(col("category"))
    )
    # 8. Drop products with no category
    .filter(col("category").isNotNull() & (col("category") != ""))
    # 9. Add processing timestamp
    .withColumn("silver_timestamp", current_timestamp())
)

silver_products.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.silver_products")
print(f"✅ Silver products: {silver_products.count()} rows (from {bronze_products.count()} bronze)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Silver Orders

# COMMAND ----------
bronze_orders = spark.table(f"{catalog}.{schema}.bronze_orders")

# Get valid customer IDs for referential integrity check
valid_customer_ids = (
    spark.table(f"{catalog}.{schema}.silver_customers")
    .select("customer_id")
)

# Quarantine: orders with invalid customer_id (e.g., C9999)
quarantine_orders = (
    bronze_orders
    .join(valid_customer_ids, "customer_id", "left_anti")
    .withColumn("rejection_reason", lit("invalid_customer_id"))
    .withColumn("source_table", lit("bronze_orders"))
    .withColumn("quarantine_timestamp", current_timestamp())
)

silver_orders = (
    bronze_orders
    # 1. Deduplicate on order_id
    .dropDuplicates(["order_id"])
    # 2. Drop nulls on primary key
    .filter(col("order_id").isNotNull())
    # 3. Keep only orders with valid customer_id (referential integrity)
    .join(valid_customer_ids, "customer_id", "inner")
    # 4. Parse order_date — handle multiple formats safely with try_to_date
    .withColumn("order_date",
        coalesce(
            expr("try_to_date(order_date, 'yyyy-MM-dd')"),
            expr("try_to_date(order_date, 'M/d/yyyy')"),
            expr("try_to_date(order_date, 'd.M.yyyy')"),
            expr("try_to_date(order_date, 'dd.MM.yyyy')")
        )
    )
    # 5. Standardize status to lowercase
    .withColumn("status",
        when(lower(trim(col("status"))) == "cancelled", "cancelled")
        .when(lower(trim(col("status"))) == "delivered", "delivered")
        .when(lower(trim(col("status"))) == "shipped", "shipped")
        .when(lower(trim(col("status"))) == "pending", "pending")
        .when(lower(trim(col("status"))) == "returned", "returned")
        .otherwise(lower(trim(col("status"))))
    )
    # 6. Standardize payment_method
    .withColumn("payment_method", trim(col("payment_method")))
    .withColumn("payment_method",
        when(lower(col("payment_method")).isin("cod", "cash on delivery"), "COD")
        .when(lower(col("payment_method")) == "credit card", "Credit Card")
        .when(lower(col("payment_method")) == "debit card", "Debit Card")
        .when(lower(col("payment_method")) == "paypal", "PayPal")
        .when(lower(col("payment_method")) == "upi", "UPI")
        .otherwise(col("payment_method"))
    )
    # 7. Standardize shipping_city
    .withColumn("shipping_city", trim(initcap(lower(col("shipping_city")))))
    .withColumn("shipping_city",
        when(lower(col("shipping_city")).isin("bangalore", "bengaluru"), "Bengaluru")
        .otherwise(col("shipping_city"))
    )
    # 8. Add processing timestamp
    .withColumn("silver_timestamp", current_timestamp())
)

silver_orders.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.silver_orders")
print(f"✅ Silver orders: {silver_orders.count()} rows (from {bronze_orders.count()} bronze)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Silver Order Items

# COMMAND ----------
bronze_order_items = spark.table(f"{catalog}.{schema}.bronze_order_items")

# Get valid order IDs and product IDs for referential integrity
valid_order_ids = spark.table(f"{catalog}.{schema}.silver_orders").select("order_id")
valid_product_ids = spark.table(f"{catalog}.{schema}.silver_products").select("product_id")

# Quarantine: order items with orphan references or bad data
quarantine_items_orphan_order = (
    bronze_order_items
    .filter(col("order_id").isNotNull())
    .join(valid_order_ids, "order_id", "left_anti")
    .withColumn("rejection_reason", lit("invalid_order_id"))
    .withColumn("source_table", lit("bronze_order_items"))
    .withColumn("quarantine_timestamp", current_timestamp())
)

quarantine_items_orphan_product = (
    bronze_order_items
    .filter(col("product_id").isNotNull())
    .join(valid_product_ids, "product_id", "left_anti")
    .withColumn("rejection_reason", lit("invalid_product_id"))
    .withColumn("source_table", lit("bronze_order_items"))
    .withColumn("quarantine_timestamp", current_timestamp())
)

silver_order_items = (
    bronze_order_items
    # 1. Deduplicate on order_item_id
    .dropDuplicates(["order_item_id"])
    # 2. Drop nulls on primary key and foreign keys
    .filter(
        col("order_item_id").isNotNull() &
        col("order_id").isNotNull() &
        col("product_id").isNotNull()
    )
    # 3. Referential integrity — keep only valid orders and products
    .join(valid_order_ids, "order_id", "inner")
    .join(valid_product_ids, "product_id", "inner")
    # 4. Clean unit_price — handle "N/A", negatives
    .withColumn("unit_price",
        when(lower(col("unit_price")).isin("n/a", ""), None)
        .otherwise(col("unit_price"))
    )
    .withColumn("unit_price", expr("try_cast(unit_price as double)"))
    .withColumn("unit_price",
        when(col("unit_price") < 0, None).otherwise(col("unit_price"))
    )
    # 5. Clean quantity — handle nulls, negatives, zeros
    .withColumn("quantity", expr("try_cast(quantity as int)"))
    .filter(col("quantity").isNotNull() & (col("quantity") > 0))
    # 6. Filter out rows where unit_price is null (can't calculate revenue)
    .filter(col("unit_price").isNotNull())
    # 7. Add processing timestamp
    .withColumn("silver_timestamp", current_timestamp())
)

silver_order_items.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.silver_order_items")
print(f"Silver order_items: {silver_order_items.count()} rows (from {bronze_order_items.count()} bronze)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Write Quarantine Table

# COMMAND ----------
# Combine all quarantined records
# Select common columns for union
quarantine_cols = ["rejection_reason", "source_table", "quarantine_timestamp"]

quarantine_all = (
    quarantine_orders.select(
        col("order_id").cast("string").alias("record_id"),
        col("customer_id").cast("string").alias("related_id"),
        *quarantine_cols
    )
    .unionByName(
        quarantine_items_orphan_order.select(
            col("order_item_id").cast("string").alias("record_id"),
            col("order_id").cast("string").alias("related_id"),
            *quarantine_cols
        )
    )
    .unionByName(
        quarantine_items_orphan_product.select(
            col("order_item_id").cast("string").alias("record_id"),
            col("product_id").cast("string").alias("related_id"),
            *quarantine_cols
        )
    )
)

quarantine_all.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.silver_quarantine")
print(f"🚫 Quarantine: {quarantine_all.count()} rejected records")

# COMMAND ----------
# Summary
print("\n" + "="*60)
print("SILVER LAYER COMPLETE")
print("="*60)
print(f"  silver_customers:   {spark.table(f'{catalog}.{schema}.silver_customers').count()} rows")
print(f"  silver_products:    {spark.table(f'{catalog}.{schema}.silver_products').count()} rows")
print(f"  silver_orders:      {spark.table(f'{catalog}.{schema}.silver_orders').count()} rows")
print(f"  silver_order_items: {spark.table(f'{catalog}.{schema}.silver_order_items').count()} rows")
print(f"  silver_quarantine:  {spark.table(f'{catalog}.{schema}.silver_quarantine').count()} rows")
print("="*60)
