# Fabric Notebook: 03_silver_to_gold
# Phase 3 - Gold layer: star schema (dimensions + facts), DimAsset as SCD Type 2
# Run this in a Fabric notebook attached to the same Lakehouse as the Bronze/Silver notebooks.

# CELL 1 - Imports
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# CELL 2 - Load Silver tables
silver_branches = spark.read.table("silver_branches")
silver_assets = spark.read.table("silver_assets")
silver_customers = spark.read.table("silver_customers")
silver_contracts = spark.read.table("silver_rental_contracts")
silver_workorders = spark.read.table("silver_work_orders")

# CELL 3 - DimDate (generated, covers 2015-01-01 to 2027-12-31)
date_df = spark.sql("""
    SELECT explode(sequence(to_date('2015-01-01'), to_date('2027-12-31'), interval 1 day)) as full_date
""")

dim_date = (date_df
    .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("full_date"))
    .withColumn("month", F.month("full_date"))
    .withColumn("month_name", F.date_format("full_date", "MMMM"))
    .withColumn("quarter", F.quarter("full_date"))
    .withColumn("day_of_week", F.date_format("full_date", "EEEE"))
    .withColumn("is_weekend", F.dayofweek("full_date").isin([1, 7]))
)

# CELL 4 - DimBranch (simple dimension, one row per branch)
dim_branch = (silver_branches
    .withColumnRenamed("branch_id", "branch_key")
    .select("branch_key", "branch_name", "region", "opened_date", "manager_name")
)

# CELL 5 - DimCustomer (simple dimension, one row per customer)
dim_customer = (silver_customers
    .withColumnRenamed("customer_id", "customer_key")
    .select("customer_key", "customer_name", "customer_type", "signup_date", "county")
)

# CELL 6 - DimAsset as SCD Type 2
# On first run, every asset gets one "current" version. On later runs, this cell detects
# changes (e.g. status or home_branch_id changed) and expires the old row while inserting a new one.

silver_assets_prepped = (silver_assets
    .select("asset_id", "asset_class", "manufacturer", "model_year",
            "purchase_cost", "home_branch_id", "status")
)

gold_table_exists = spark.catalog.tableExists("dim_asset")

if not gold_table_exists:
    # First run: initialize SCD2 structure
    dim_asset = (silver_assets_prepped
        .withColumn("surrogate_key", F.monotonically_increasing_id())
        .withColumn("effective_date", F.lit("2015-01-01").cast("date"))
        .withColumn("end_date", F.lit(None).cast("date"))
        .withColumn("is_current", F.lit(True))
    )
    dim_asset.write.mode("overwrite").format("delta").saveAsTable("dim_asset")
    print(f"DimAsset initialized with {dim_asset.count()} current rows (SCD2 structure).")
else:
    # Subsequent run: merge - expire changed rows, insert new versions
    existing = DeltaTable.forName(spark, "dim_asset")
    current_rows = existing.toDF().filter(F.col("is_current") == True)

    changed = (silver_assets_prepped.alias("new")
        .join(current_rows.alias("old"), "asset_id")
        .where(
            (F.col("new.status") != F.col("old.status")) |
            (F.col("new.home_branch_id") != F.col("old.home_branch_id"))
        )
        .select("new.*")
    )

    # Expire old versions of changed assets
    existing.alias("t").merge(
        changed.alias("s"),
        "t.asset_id = s.asset_id AND t.is_current = true"
    ).whenMatchedUpdate(set={
        "end_date": F.current_date(),
        "is_current": F.lit(False)
    }).execute()

    # Insert new versions for changed assets
    new_versions = (changed
        .withColumn("surrogate_key", F.monotonically_increasing_id())
        .withColumn("effective_date", F.current_date())
        .withColumn("end_date", F.lit(None).cast("date"))
        .withColumn("is_current", F.lit(True))
    )
    new_versions.write.mode("append").format("delta").saveAsTable("dim_asset")
    print(f"DimAsset updated: {new_versions.count()} assets got a new SCD2 version.")

# CELL 7 - FactRentalContract (grain: one row per rental contract)
fact_rental_contract = (silver_contracts
    .withColumn("start_date_key", F.date_format("rental_start", "yyyyMMdd").cast("int"))
    .withColumn("end_date_key", F.date_format("rental_end", "yyyyMMdd").cast("int"))
    .select(
        "contract_id", "asset_id", "customer_id", "branch_id",
        "start_date_key", "end_date_key", "duration_days",
        "daily_rate", "total_revenue", "on_time_return"
    )
)

# CELL 8 - FactMaintenance (grain: one row per work order)
fact_maintenance = (silver_workorders
    .withColumn("opened_date_key", F.date_format("opened_date", "yyyyMMdd").cast("int"))
    .select(
        "work_order_id", "asset_id", "issue_type",
        "opened_date_key", "repair_cost", "downtime_hours"
    )
)

# CELL 9 - Write Gold tables
dim_date.write.mode("overwrite").format("delta").saveAsTable("dim_date")
dim_branch.write.mode("overwrite").format("delta").saveAsTable("dim_branch")
dim_customer.write.mode("overwrite").format("delta").saveAsTable("dim_customer")
fact_rental_contract.write.mode("overwrite").format("delta").saveAsTable("fact_rental_contract")
fact_maintenance.write.mode("overwrite").format("delta").saveAsTable("fact_maintenance")

print("Gold layer complete: dim_date, dim_branch, dim_customer, dim_asset (SCD2), fact_rental_contract, fact_maintenance")

# CELL 10 - Quick sanity check
print("\nRow counts:")
for name in ["dim_date", "dim_branch", "dim_customer", "dim_asset", "fact_rental_contract", "fact_maintenance"]:
    print(f"  {name}: {spark.read.table(name).count()}")
