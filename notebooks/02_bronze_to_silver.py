# Fabric Notebook: 02_bronze_to_silver
# Phase 3 - Silver layer: cleanse, dedupe, enforce schema, quarantine bad rows
# Run this in a Fabric notebook attached to the same Lakehouse as the Bronze notebook.

# CELL 1 - Imports
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable

# CELL 2 - Load Bronze tables
bronze_branches = spark.read.table("bronze_branches")
bronze_assets = spark.read.table("bronze_assets")
bronze_customers = spark.read.table("bronze_customers")
bronze_contracts = spark.read.table("bronze_rental_contracts")
bronze_workorders = spark.read.table("bronze_work_orders")

print("Bronze row counts:")
for name, df in [("branches", bronze_branches), ("assets", bronze_assets),
                  ("customers", bronze_customers), ("contracts", bronze_contracts),
                  ("work_orders", bronze_workorders)]:
    print(f"  {name}: {df.count()}")

# CELL 3 - Silver: Branches (dedupe on branch_id, drop nulls in key columns)
silver_branches = (bronze_branches
    .dropDuplicates(["branch_id"])
    .filter(F.col("branch_id").isNotNull() & F.col("branch_name").isNotNull())
    .withColumn("_silver_loaded_at", F.current_timestamp())
)

# CELL 4 - Silver: Assets (dedupe, enforce valid status values, flag bad rows to quarantine)
valid_statuses = ["Active", "In Maintenance", "Retired"]

assets_checked = bronze_assets.withColumn(
    "_dq_valid",
    (F.col("asset_id").isNotNull()) &
    (F.col("purchase_cost") > 0) &
    (F.col("status").isin(valid_statuses)) &
    (F.col("home_branch_id").isNotNull())
)

silver_assets = (assets_checked
    .filter(F.col("_dq_valid"))
    .dropDuplicates(["asset_id"])
    .drop("_dq_valid")
    .withColumn("_silver_loaded_at", F.current_timestamp())
)

quarantine_assets = assets_checked.filter(~F.col("_dq_valid"))
print(f"Assets: {silver_assets.count()} passed, {quarantine_assets.count()} quarantined")

# CELL 5 - Silver: Customers (dedupe, trim whitespace, drop rows with no name)
silver_customers = (bronze_customers
    .withColumn("customer_name", F.trim(F.col("customer_name")))
    .filter(F.col("customer_name").isNotNull() & (F.col("customer_name") != ""))
    .dropDuplicates(["customer_id"])
    .withColumn("_silver_loaded_at", F.current_timestamp())
)

# CELL 6 - Silver: Rental contracts (data quality checks + quarantine)
contracts_checked = bronze_contracts.withColumn(
    "_dq_valid",
    (F.col("contract_id").isNotNull()) &
    (F.col("rental_end") >= F.col("rental_start")) &
    (F.col("total_revenue") >= 0) &
    (F.col("duration_days") > 0)
)

silver_contracts = (contracts_checked
    .filter(F.col("_dq_valid"))
    .dropDuplicates(["contract_id"])
    .drop("_dq_valid")
    .withColumn("_silver_loaded_at", F.current_timestamp())
)

quarantine_contracts = contracts_checked.filter(~F.col("_dq_valid"))
print(f"Contracts: {silver_contracts.count()} passed, {quarantine_contracts.count()} quarantined")

# CELL 7 - Silver: Work orders (dedupe, drop negative costs/downtime as invalid)
workorders_checked = bronze_workorders.withColumn(
    "_dq_valid",
    (F.col("work_order_id").isNotNull()) &
    (F.col("repair_cost") >= 0) &
    (F.col("downtime_hours") >= 0)
)

silver_workorders = (workorders_checked
    .filter(F.col("_dq_valid"))
    .dropDuplicates(["work_order_id"])
    .drop("_dq_valid")
    .withColumn("_silver_loaded_at", F.current_timestamp())
)

quarantine_workorders = workorders_checked.filter(~F.col("_dq_valid"))
print(f"Work orders: {silver_workorders.count()} passed, {quarantine_workorders.count()} quarantined")

# CELL 8 - Write Silver tables
silver_branches.write.mode("overwrite").format("delta").saveAsTable("silver_branches")
silver_assets.write.mode("overwrite").format("delta").saveAsTable("silver_assets")
silver_customers.write.mode("overwrite").format("delta").saveAsTable("silver_customers")
silver_contracts.write.mode("overwrite").format("delta").saveAsTable("silver_rental_contracts")
silver_workorders.write.mode("overwrite").format("delta").saveAsTable("silver_work_orders")

# CELL 9 - Write quarantine tables (so bad rows are visible, not silently dropped)
quarantine_assets.write.mode("overwrite").format("delta").saveAsTable("quarantine_assets")
quarantine_contracts.write.mode("overwrite").format("delta").saveAsTable("quarantine_rental_contracts")
quarantine_workorders.write.mode("overwrite").format("delta").saveAsTable("quarantine_work_orders")

print("Silver layer complete. Quarantine tables hold rows that failed data quality checks for review.")
