# Fabric Notebook: 01_generate_synthetic_data
# Phase 2 - Synthetic data generation for the Fleet Utilisation & Maintenance Analytics project
# Run this in a Fabric notebook attached to a Lakehouse. Each "# CELL n" marks where to split into separate notebook cells.

# CELL 1 - Install/import
%pip install faker
from faker import Faker
import numpy as np
import pandas as pd
import random
from datetime import datetime, timedelta

fake = Faker()
Faker.seed(42)
np.random.seed(42)
random.seed(42)

N_BRANCHES = 40
N_ASSETS = 3000
N_CUSTOMERS = 5000
N_CONTRACTS = 200000   # start smaller (200k), scale to 2M later once the pipeline is proven
N_WORKORDERS = 15000

# CELL 2 - Branches
asset_classes = ["Excavator", "Skid Steer", "Generator", "Scissor Lift", "Boom Lift",
                  "Compressor", "Forklift", "Telehandler", "Dumper", "Compactor"]

branches = pd.DataFrame({
    "branch_id": range(1, N_BRANCHES + 1),
    "branch_name": [f"{fake.city()} Depot" for _ in range(N_BRANCHES)],
    "region": np.random.choice(["Leinster", "Munster", "Connacht", "Ulster"], N_BRANCHES),
    "opened_date": [fake.date_between(start_date="-10y", end_date="-1y") for _ in range(N_BRANCHES)],
    "manager_name": [fake.name() for _ in range(N_BRANCHES)]
})

# CELL 3 - Assets (equipment fleet)
assets = pd.DataFrame({
    "asset_id": range(1, N_ASSETS + 1),
    "asset_class": np.random.choice(asset_classes, N_ASSETS),
    "manufacturer": np.random.choice(["Caterpillar", "JCB", "Bobcat", "Komatsu", "Volvo", "Hitachi"], N_ASSETS),
    "model_year": np.random.randint(2015, 2026, N_ASSETS),
    "purchase_cost": np.round(np.random.uniform(8000, 150000, N_ASSETS), 2),
    "home_branch_id": np.random.randint(1, N_BRANCHES + 1, N_ASSETS),
    "status": np.random.choice(["Active", "In Maintenance", "Retired"], N_ASSETS, p=[0.85, 0.10, 0.05])
})

# CELL 4 - Customers
customers = pd.DataFrame({
    "customer_id": range(1, N_CUSTOMERS + 1),
    "customer_name": [fake.company() for _ in range(N_CUSTOMERS)],
    "customer_type": np.random.choice(["Construction", "Landscaping", "Events", "Utilities", "Government"], N_CUSTOMERS),
    "signup_date": [fake.date_between(start_date="-8y", end_date="today") for _ in range(N_CUSTOMERS)],
    "county": [fake.city() for _ in range(N_CUSTOMERS)]
})

# CELL 5 - Rental contracts (the main fact table source)
def random_contract(i):
    start = fake.date_between(start_date="-2y", end_date="today")
    duration = np.random.choice([1, 3, 7, 14, 30, 90], p=[0.30, 0.25, 0.20, 0.12, 0.10, 0.03])
    end = start + timedelta(days=int(duration))
    asset_id = np.random.randint(1, N_ASSETS + 1)
    daily_rate = np.round(np.random.uniform(80, 900), 2)
    return {
        "contract_id": i,
        "asset_id": asset_id,
        "customer_id": np.random.randint(1, N_CUSTOMERS + 1),
        "branch_id": np.random.randint(1, N_BRANCHES + 1),
        "rental_start": start,
        "rental_end": end,
        "duration_days": duration,
        "daily_rate": daily_rate,
        "total_revenue": np.round(daily_rate * duration, 2),
        "on_time_return": np.random.choice([True, False], p=[0.88, 0.12])
    }

contracts = pd.DataFrame([random_contract(i) for i in range(1, N_CONTRACTS + 1)])

# CELL 6 - Work orders (maintenance events)
work_orders = pd.DataFrame({
    "work_order_id": range(1, N_WORKORDERS + 1),
    "asset_id": np.random.randint(1, N_ASSETS + 1, N_WORKORDERS),
    "issue_type": np.random.choice(
        ["Engine", "Hydraulics", "Electrical", "Tires/Tracks", "Routine Service"], N_WORKORDERS),
    "opened_date": [fake.date_between(start_date="-2y", end_date="today") for _ in range(N_WORKORDERS)],
    "repair_cost": np.round(np.random.exponential(scale=350, size=N_WORKORDERS), 2),
    "downtime_hours": np.round(np.random.exponential(scale=12, size=N_WORKORDERS), 1)
})

# CELL 7 - Write to Lakehouse as Delta tables (Bronze layer)
# Adjust "Tables/bronze_*" if your lakehouse uses a different naming convention.
spark_branches = spark.createDataFrame(branches)
spark_assets = spark.createDataFrame(assets)
spark_customers = spark.createDataFrame(customers)
spark_contracts = spark.createDataFrame(contracts)
spark_workorders = spark.createDataFrame(work_orders)

spark_branches.write.mode("overwrite").format("delta").saveAsTable("bronze_branches")
spark_assets.write.mode("overwrite").format("delta").saveAsTable("bronze_assets")
spark_customers.write.mode("overwrite").format("delta").saveAsTable("bronze_customers")
spark_contracts.write.mode("overwrite").format("delta").saveAsTable("bronze_rental_contracts")
spark_workorders.write.mode("overwrite").format("delta").saveAsTable("bronze_work_orders")

print("Bronze tables written: bronze_branches, bronze_assets, bronze_customers, bronze_rental_contracts, bronze_work_orders")
print(f"Row counts -> branches: {branches.shape[0]}, assets: {assets.shape[0]}, customers: {customers.shape[0]}, contracts: {contracts.shape[0]}, work_orders: {work_orders.shape[0]}")
