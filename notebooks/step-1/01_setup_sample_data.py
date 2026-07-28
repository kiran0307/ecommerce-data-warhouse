# notebooks/step-1/01_Setup_Sample_Data.py
# Purpose: Setup and generate sample E-commerce data
# Version: 1.0

import pandas as pd
from datetime import datetime, timedelta
import random
import numpy as np

random.seed(42)
np.random.seed(42)

print("=" * 60)
print("STEP 1: Generating Sample E-Commerce Data")
print("=" * 60)

# ========== CUSTOMERS DATA ==========
print("\n[1] Generating Customers data...")
customer_data = {
    'customer_id': range(1, 1001),
    'customer_name': [f'Customer_{i}' for i in range(1, 1001)],
    'email': [f'cust_{i}@email.com' for i in range(1, 1001)],
    'country': [random.choice(['USA', 'UK', 'Canada', 'Australia', 'Germany']) for _ in range(1000)],
    'city': [random.choice(['New York', 'London', 'Toronto', 'Sydney', 'Berlin']) for _ in range(1000)],
    'signup_date': [
        (datetime.now() - timedelta(days=random.randint(1, 730))).strftime('%Y-%m-%d') 
        for _ in range(1000)
    ]
}
customers_df = pd.DataFrame(customer_data)
print(f"✓ Created {len(customers_df)} customers")
print(f"  Columns: {list(customers_df.columns)}")

# ========== PRODUCTS DATA ==========
print("\n[2] Generating Products data...")
product_categories = ['Electronics', 'Clothing', 'Books', 'Home & Garden', 'Sports']
product_data = {
    'product_id': range(1, 101),
    'product_name': [f'Product_{i}' for i in range(1, 101)],
    'category': [random.choice(product_categories) for _ in range(100)],
    'price': [round(random.uniform(10, 500), 2) for _ in range(100)],
    'stock_quantity': [random.randint(0, 500) for _ in range(100)]
}
products_df = pd.DataFrame(product_data)
print(f"✓ Created {len(products_df)} products")

# ========== ORDERS DATA ==========
print("\n[3] Generating Orders data...")
order_statuses = ['Completed', 'Pending', 'Cancelled']
order_data = {
    'order_id': range(1, 5001),
    'customer_id': [random.randint(1, 1000) for _ in range(5000)],
    'order_date': [
        (datetime.now() - timedelta(days=random.randint(1, 365))).strftime('%Y-%m-%d') 
        for _ in range(5000)
    ],
    'order_amount': [round(random.uniform(20, 2000), 2) for _ in range(5000)],
    'status': [random.choice(order_statuses) for _ in range(5000)]
}
orders_df = pd.DataFrame(order_data)
print(f"✓ Created {len(orders_df)} orders")

# ========== ORDER ITEMS DATA ==========
print("\n[4] Generating Order Items data...")
order_items_data = []
for order_id in range(1, 5001):
    num_items = random.randint(1, 5)
    for _ in range(num_items):
        order_items_data.append({
            'order_id': order_id,
            'product_id': random.randint(1, 100),
            'quantity': random.randint(1, 10),
            'unit_price': round(random.uniform(10, 500), 2)
        })
order_items_df = pd.DataFrame(order_items_data)
print(f"✓ Created {len(order_items_df)} order items")

print("\n" + "=" * 60)
print("✅ DATA GENERATION COMPLETE!")
print("=" * 60)
print(f"\nSummary:")
print(f"- Customers: {len(customers_df)}")
print(f"- Products: {len(products_df)}")
print(f"- Orders: {len(orders_df)}")
print(f"- Order Items: {len(order_items_df)}")

# Make dataframes available for next notebooks
print("\n✅ All dataframes ready for next steps!")