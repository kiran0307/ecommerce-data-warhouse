# Databricks notebook source
# ============================================================================
# E-commerce Data Warehouse - Step 2: Create Bronze Delta Tables
# ============================================================================
# Purpose: Create Bronze layer Delta tables with ingestion metadata
# Bronze tables store raw data as-is with lineage tracking
# ============================================================================

from pyspark.sql.functions import *
from datetime import datetime

# COMMAND ----------

# Configuration
CATALOG_NAME = "main"  # Update if using Unity Catalog
SCHEMA_NAME = "ecommerce_dwh"
BRONZE_SCHEMA = f"{SCHEMA_NAME}_bronze"
DATA_PATH = "/Volumes/main/default/ecommerce_data"  # Adjust based on your setup

# Create schemas if they don't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")

print(f"✓ Schema '{BRONZE_SCHEMA}' ready")

# COMMAND ----------

# ============================================================================
# Add Ingestion Metadata to Raw DataFrames
# ============================================================================

# Get reference to raw dataframes (from Step 1)
customers_raw = spark.table("customers_raw")
products_raw = spark.table("products_raw")
orders_raw = spark.table("orders_raw")
order_items_raw = spark.table("order_items_raw")

# Add ingestion metadata columns
ingestion_timestamp = current_timestamp()
ingestion_date = current_date()
source_system = lit("E-commerce App")
ingestion_method = lit("PySpark Direct Load")

# COMMAND ----------

# ============================================================================
# 1. CREATE BRONZE CUSTOMERS TABLE
# ============================================================================

print("\n[1] Creating Bronze Customers Table...")

customers_bronze = (customers_raw
    .withColumn("ingestion_timestamp", ingestion_timestamp)
    .withColumn("ingestion_date", ingestion_date)
    .withColumn("source_system", source_system)
    .withColumn("ingestion_method", ingestion_method)
    .withColumn("data_quality_flag", lit("PENDING"))
    .withColumn("record_hash", md5(concat_ws("|", 
        col("customer_id"), 
        col("first_name"), 
        col("last_name"), 
        col("email"))))
)

# Create or replace Bronze Customers table
customers_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{BRONZE_SCHEMA}.customers")

print(f"✓ Created Bronze Customers table: {BRONZE_SCHEMA}.customers")
print(f"  Rows: {customers_bronze.count()}")

# COMMAND ----------

# ============================================================================
# 2. CREATE BRONZE PRODUCTS TABLE
# ============================================================================

print("\n[2] Creating Bronze Products Table...")

products_bronze = (products_raw
    .withColumn("ingestion_timestamp", ingestion_timestamp)
    .withColumn("ingestion_date", ingestion_date)
    .withColumn("source_system", source_system)
    .withColumn("ingestion_method", ingestion_method)
    .withColumn("data_quality_flag", lit("PENDING"))
    .withColumn("record_hash", md5(concat_ws("|",
        col("product_id"),
        col("product_name"),
        col("category"),
        col("price"))))
)

# Create or replace Bronze Products table
products_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{BRONZE_SCHEMA}.products")

print(f"✓ Created Bronze Products table: {BRONZE_SCHEMA}.products")
print(f"  Rows: {products_bronze.count()}")

# COMMAND ----------

# ============================================================================
# 3. CREATE BRONZE ORDERS TABLE
# ============================================================================

print("\n[3] Creating Bronze Orders Table...")

orders_bronze = (orders_raw
    .withColumn("ingestion_timestamp", ingestion_timestamp)
    .withColumn("ingestion_date", ingestion_date)
    .withColumn("source_system", source_system)
    .withColumn("ingestion_method", ingestion_method)
    .withColumn("data_quality_flag", lit("PENDING"))
    .withColumn("record_hash", md5(concat_ws("|",
        col("order_id"),
        col("customer_id"),
        col("order_date"),
        col("total_amount"))))
)

# Create or replace Bronze Orders table
orders_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{BRONZE_SCHEMA}.orders")

print(f"✓ Created Bronze Orders table: {BRONZE_SCHEMA}.orders")
print(f"  Rows: {orders_bronze.count()}")

# COMMAND ----------

# ============================================================================
# 4. CREATE BRONZE ORDER_ITEMS TABLE
# ============================================================================

print("\n[4] Creating Bronze Order Items Table...")

order_items_bronze = (order_items_raw
    .withColumn("ingestion_timestamp", ingestion_timestamp)
    .withColumn("ingestion_date", ingestion_date)
    .withColumn("source_system", source_system)
    .withColumn("ingestion_method", ingestion_method)
    .withColumn("data_quality_flag", lit("PENDING"))
    .withColumn("record_hash", md5(concat_ws("|",
        col("item_id"),
        col("order_id"),
        col("product_id"),
        col("quantity"))))
)

# Create or replace Bronze Order Items table
order_items_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{BRONZE_SCHEMA}.order_items")

print(f"✓ Created Bronze Order Items table: {BRONZE_SCHEMA}.order_items")
print(f"  Rows: {order_items_bronze.count()}")

# COMMAND ----------

# ============================================================================
# 5. DISPLAY BRONZE TABLE SCHEMAS
# ============================================================================

print("\n" + "="*70)
print("BRONZE TABLE SCHEMAS")
print("="*70)

spark.sql(f"DESCRIBE TABLE {BRONZE_SCHEMA}.customers").display()

# COMMAND ----------

print("\n" + "="*70)
print("BRONZE CUSTOMERS TABLE SAMPLE DATA")
print("="*70)

spark.sql(f"SELECT * FROM {BRONZE_SCHEMA}.customers LIMIT 5").display()

# COMMAND ----------

print("\n" + "="*70)
print("BRONZE PRODUCTS TABLE SAMPLE DATA")
print("="*70)

spark.sql(f"SELECT * FROM {BRONZE_SCHEMA}.products LIMIT 5").display()

# COMMAND ----------

print("\n" + "="*70)
print("BRONZE ORDERS TABLE SAMPLE DATA")
print("="*70)

spark.sql(f"SELECT * FROM {BRONZE_SCHEMA}.orders LIMIT 5").display()

# COMMAND ----------

print("\n" + "="*70)
print("BRONZE ORDER_ITEMS TABLE SAMPLE DATA")
print("="*70)

spark.sql(f"SELECT * FROM {BRONZE_SCHEMA}.order_items LIMIT 5").display()

# COMMAND ----------

# ============================================================================
# 6. DATA QUALITY SUMMARY
# ============================================================================

print("\n" + "="*70)
print("DATA QUALITY SUMMARY")
print("="*70)

# Check for nulls in key columns
print("\n[CUSTOMERS] Null checks:")
spark.sql(f"""
SELECT 
    COUNT(*) as total_rows,
    COUNTIF(customer_id IS NULL) as null_customer_id,
    COUNTIF(email IS NULL) as null_email
FROM {BRONZE_SCHEMA}.customers
""").display()

# COMMAND ----------

print("\n[PRODUCTS] Null checks:")
spark.sql(f"""
SELECT 
    COUNT(*) as total_rows,
    COUNTIF(product_id IS NULL) as null_product_id,
    COUNTIF(price IS NULL) as null_price
FROM {BRONZE_SCHEMA}.products
""").display()

# COMMAND ----------

print("\n[ORDERS] Null checks:")
spark.sql(f"""
SELECT 
    COUNT(*) as total_rows,
    COUNTIF(order_id IS NULL) as null_order_id,
    COUNTIF(customer_id IS NULL) as null_customer_id
FROM {BRONZE_SCHEMA}.orders
""").display()

# COMMAND ----------

print("\n[ORDER_ITEMS] Null checks:")
spark.sql(f"""
SELECT 
    COUNT(*) as total_rows,
    COUNTIF(item_id IS NULL) as null_item_id,
    COUNTIF(order_id IS NULL) as null_order_id
FROM {BRONZE_SCHEMA}.order_items
""").display()

# COMMAND ----------

# ============================================================================
# 7. LIST ALL BRONZE TABLES
# ============================================================================

print("\n" + "="*70)
print("BRONZE LAYER TABLES CREATED")
print("="*70)

spark.sql(f"SHOW TABLES IN {BRONZE_SCHEMA}").display()

# COMMAND ----------

print("\n" + "="*70)
print("✅ BRONZE LAYER CREATION COMPLETE!")
print("="*70)
print(f"\nTables created in schema: {BRONZE_SCHEMA}")
print("\nTables:")
print(f"  1. {BRONZE_SCHEMA}.customers")
print(f"  2. {BRONZE_SCHEMA}.products")
print(f"  3. {BRONZE_SCHEMA}.orders")
print(f"  4. {BRONZE_SCHEMA}.order_items")
print("\nEach table includes:")
print("  - Original data columns")
print("  - ingestion_timestamp (when data was loaded)")
print("  - ingestion_date (date of load)")
print("  - source_system (origin)")
print("  - ingestion_method (how it was loaded)")
print("  - data_quality_flag (for validation)")
print("  - record_hash (for duplicate detection)")