# Databricks notebook source
# ============================================================================
# E-commerce Data Warehouse - Step 2: Create Bronze Delta Tables
# ============================================================================
# Purpose: Create Bronze layer Delta tables with ingestion metadata
# Bronze tables store raw data as-is with lineage tracking
# ============================================================================

from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import random

# COMMAND ----------

# ============================================================================
# STEP 1: GENERATE SAMPLE DATA (from Step 1)
# ============================================================================

print("\n" + "="*70)
print("GENERATING SAMPLE E-COMMERCE DATA")
print("="*70)

def generate_customers_data(num_customers=1000):
    customer_data = []
    first_names = ["John", "Jane", "Michael", "Sarah", "David", "Emily", "Robert", "Lisa", "James", "Mary"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"]
    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"]
    states = ["NY", "CA", "IL", "TX", "AZ", "PA", "TX", "CA", "TX", "CA"]
    
    for i in range(num_customers):
        customer_id = f"CUST{i+1:06d}"
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        email = f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 9999)}@email.com"
        city = random.choice(cities)
        state = random.choice(states)
        zip_code = f"{random.randint(10000, 99999)}"
        
        customer_data.append({
            "customer_id": customer_id,
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "city": city,
            "state": state,
            "zip_code": zip_code,
            "created_date": datetime(2023, 1, 1) + timedelta(days=random.randint(0, 365))
        })
    
    return customer_data

def generate_products_data(num_products=500):
    product_data = []
    categories = ["Electronics", "Clothing", "Home & Garden", "Sports", "Books", "Beauty", "Toys", "Food"]
    brands = ["TechBrand", "FashionCo", "HomeMax", "SportsPro", "BookWorld", "BeautyPlus", "ToyJoy", "FoodFresh"]
    
    for i in range(num_products):
        product_id = f"PROD{i+1:06d}"
        product_name = f"Product {random.choice(categories)} {i+1}"
        category = random.choice(categories)
        brand = random.choice(brands)
        # FIX: Convert to float explicitly
        price = float(round(random.uniform(5.0, 500.0), 2))
        cost = float(round(price * random.uniform(0.3, 0.7), 2))
        
        product_data.append({
            "product_id": product_id,
            "product_name": product_name,
            "category": category,
            "brand": brand,
            "price": price,
            "cost": cost,
            "stock_quantity": int(random.randint(0, 1000)),
            "created_date": datetime(2023, 1, 1) + timedelta(days=random.randint(0, 365))
        })
    
    return product_data

def generate_orders_data(num_orders=5000, num_customers=1000):
    order_data = []
    statuses = ["Completed", "Pending", "Shipped", "Cancelled", "Returned"]
    
    for i in range(num_orders):
        order_id = f"ORD{i+1:08d}"
        customer_id = f"CUST{random.randint(1, num_customers):06d}"
        order_date = datetime(2023, 1, 1) + timedelta(days=random.randint(0, 365))
        status = random.choice(statuses)
        # FIX: Convert to float explicitly
        total_amount = float(round(random.uniform(10.0, 1000.0), 2))
        
        order_data.append({
            "order_id": order_id,
            "customer_id": customer_id,
            "order_date": order_date,
            "status": status,
            "total_amount": total_amount,
            "payment_method": random.choice(["Credit Card", "Debit Card", "PayPal", "Cryptocurrency"])
        })
    
    return order_data

def generate_order_items_data(num_orders=5000, num_products=500):
    order_items_data = []
    item_counter = 0
    
    for order_num in range(num_orders):
        order_id = f"ORD{order_num+1:08d}"
        num_items = random.randint(1, 5)
        
        for item_num in range(num_items):
            item_id = f"ITEM{item_counter+1:10d}"
            product_id = f"PROD{random.randint(1, num_products):06d}"
            quantity = int(random.randint(1, 10))
            # FIX: Convert to float explicitly
            unit_price = float(round(random.uniform(5.0, 500.0), 2))
            discount = float(round(random.uniform(0.0, 0.2), 2))
            line_total = float(round((quantity * unit_price) * (1 - discount), 2))
            
            order_items_data.append({
                "item_id": item_id,
                "order_id": order_id,
                "product_id": product_id,
                "quantity": quantity,
                "unit_price": unit_price,
                "discount": discount,
                "line_total": line_total
            })
            
            item_counter += 1
    
    return order_items_data

# COMMAND ----------

print("[1] Generating Customers data...")
customers = generate_customers_data(1000)
print(f"✓ Generated {len(customers)} customers")

print("\n[2] Generating Products data...")
products = generate_products_data(500)
print(f"✓ Generated {len(products)} products")

print("\n[3] Generating Orders data...")
orders = generate_orders_data(5000, 1000)
print(f"✓ Generated {len(orders)} orders")

print("\n[4] Generating Order Items data...")
order_items = generate_order_items_data(5000, 500)
print(f"✓ Generated {len(order_items)} order items")

# COMMAND ----------

# Create Spark DataFrames with explicit schemas
customers_raw = spark.createDataFrame(customers, schema=StructType([
    StructField("customer_id", StringType()),
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("email", StringType()),
    StructField("city", StringType()),
    StructField("state", StringType()),
    StructField("zip_code", StringType()),
    StructField("created_date", TimestampType())
]))

products_raw = spark.createDataFrame(products, schema=StructType([
    StructField("product_id", StringType()),
    StructField("product_name", StringType()),
    StructField("category", StringType()),
    StructField("brand", StringType()),
    StructField("price", DoubleType()),
    StructField("cost", DoubleType()),
    StructField("stock_quantity", IntegerType()),
    StructField("created_date", TimestampType())
]))

orders_raw = spark.createDataFrame(orders, schema=StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("order_date", TimestampType()),
    StructField("status", StringType()),
    StructField("total_amount", DoubleType()),
    StructField("payment_method", StringType())
]))

order_items_raw = spark.createDataFrame(order_items, schema=StructType([
    StructField("item_id", StringType()),
    StructField("order_id", StringType()),
    StructField("product_id", StringType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DoubleType()),
    StructField("discount", DoubleType()),
    StructField("line_total", DoubleType())
]))

print("\n✓ All DataFrames created successfully!")

# COMMAND ----------

# ============================================================================
# STEP 2: CREATE BRONZE DELTA TABLES WITH INGESTION METADATA
# ============================================================================

print("\n" + "="*70)
print("CREATING BRONZE DELTA TABLES")
print("="*70)

# Configuration
CATALOG_NAME = "main"
SCHEMA_NAME = "ecommerce_dwh"
BRONZE_SCHEMA = f"{SCHEMA_NAME}_bronze"

# Create schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")
print(f"\n✓ Schema '{BRONZE_SCHEMA}' ready")

# COMMAND ----------

# Add ingestion metadata
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

customers_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{BRONZE_SCHEMA}.customers")

print(f"✓ Created Bronze Customers table: {BRONZE_SCHEMA}.customers")
print(f"  Total rows: {customers_bronze.count()}")

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

products_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{BRONZE_SCHEMA}.products")

print(f"✓ Created Bronze Products table: {BRONZE_SCHEMA}.products")
print(f"  Total rows: {products_bronze.count()}")

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

orders_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{BRONZE_SCHEMA}.orders")

print(f"✓ Created Bronze Orders table: {BRONZE_SCHEMA}.orders")
print(f"  Total rows: {orders_bronze.count()}")

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

order_items_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{BRONZE_SCHEMA}.order_items")

print(f"✓ Created Bronze Order Items table: {BRONZE_SCHEMA}.order_items")
print(f"  Total rows: {order_items_bronze.count()}")

# COMMAND ----------

# ============================================================================
# VERIFY BRONZE TABLES
# ============================================================================

print("\n" + "="*70)
print("BRONZE TABLE CREATION SUMMARY")
print("="*70)

spark.sql(f"SHOW TABLES IN {BRONZE_SCHEMA}").display()

# COMMAND ----------

print("\n" + "="*70)
print("SAMPLE DATA FROM BRONZE CUSTOMERS")
print("="*70)

spark.sql(f"SELECT * FROM {BRONZE_SCHEMA}.customers LIMIT 5").display()

# COMMAND ----------

print("\n" + "="*70)
print("SAMPLE DATA FROM BRONZE PRODUCTS")
print("="*70)

spark.sql(f"SELECT * FROM {BRONZE_SCHEMA}.products LIMIT 5").display()

# COMMAND ----------

print("\n" + "="*70)
print("SAMPLE DATA FROM BRONZE ORDERS")
print("="*70)

spark.sql(f"SELECT * FROM {BRONZE_SCHEMA}.orders LIMIT 5").display()

# COMMAND ----------

print("\n" + "="*70)
print("SAMPLE DATA FROM BRONZE ORDER_ITEMS")
print("="*70)

spark.sql(f"SELECT * FROM {BRONZE_SCHEMA}.order_items LIMIT 5").display()

# COMMAND ----------

# ============================================================================
# DATA QUALITY CHECKS
# ============================================================================

print("\n" + "="*70)
print("DATA QUALITY CHECKS - NULL VALUES")
print("="*70)

print("\n[CUSTOMERS] Null Value Counts:")
spark.sql(f"""
SELECT 
    COUNT(*) as total_rows,
    COUNTIF(customer_id IS NULL) as null_customer_id,
    COUNTIF(email IS NULL) as null_email,
    COUNTIF(first_name IS NULL) as null_first_name
FROM {BRONZE_SCHEMA}.customers
""").display()

# COMMAND ----------

print("\n[PRODUCTS] Null Value Counts:")
spark.sql(f"""
SELECT 
    COUNT(*) as total_rows,
    COUNTIF(product_id IS NULL) as null_product_id,
    COUNTIF(price IS NULL) as null_price,
    COUNTIF(product_name IS NULL) as null_product_name
FROM {BRONZE_SCHEMA}.products
""").display()

# COMMAND ----------

print("\n[ORDERS] Null Value Counts:")
spark.sql(f"""
SELECT 
    COUNT(*) as total_rows,
    COUNTIF(order_id IS NULL) as null_order_id,
    COUNTIF(customer_id IS NULL) as null_customer_id,
    COUNTIF(total_amount IS NULL) as null_total_amount
FROM {BRONZE_SCHEMA}.orders
""").display()

# COMMAND ----------

print("\n[ORDER_ITEMS] Null Value Counts:")
spark.sql(f"""
SELECT 
    COUNT(*) as total_rows,
    COUNTIF(item_id IS NULL) as null_item_id,
    COUNTIF(order_id IS NULL) as null_order_id,
    COUNTIF(product_id IS NULL) as null_product_id
FROM {BRONZE_SCHEMA}.order_items
""").display()

# COMMAND ----------

print("\n" + "="*70)
print("✅ BRONZE LAYER CREATION COMPLETE!")
print("="*70)
print(f"\nTables created in schema: {BRONZE_SCHEMA}")
print(f"\nBronze Tables Created:")
print(f"  1. {BRONZE_SCHEMA}.customers ({customers_bronze.count()} rows)")
print(f"  2. {BRONZE_SCHEMA}.products ({products_bronze.count()} rows)")
print(f"  3. {BRONZE_SCHEMA}.orders ({orders_bronze.count()} rows)")
print(f"  4. {BRONZE_SCHEMA}.order_items ({order_items_bronze.count()} rows)")
print(f"\nMetadata columns added to all tables:")
print(f"  - ingestion_timestamp")
print(f"  - ingestion_date")
print(f"  - source_system")
print(f"  - ingestion_method")
print(f"  - data_quality_flag")
print(f"  - record_hash (for duplicate detection)")
print("\n✓ Bronze layer ready for Silver layer transformation!")