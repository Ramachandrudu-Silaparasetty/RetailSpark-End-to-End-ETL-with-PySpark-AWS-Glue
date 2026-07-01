import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# -----------------------------------------------------
# Initialize Glue
# -----------------------------------------------------

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print("========== RETAIL DATA ENGINEERING ETL STARTED ==========")

# -----------------------------------------------------
# S3 Paths
# -----------------------------------------------------

BUCKET = "retail-data-engineering-sales-data"

PROJECT = "Retail_Data_Engineering_Project"

BRONZE = f"s3://{BUCKET}/{PROJECT}/Bronze"

SILVER = f"s3://{BUCKET}/{PROJECT}/Silver"

GOLD = f"s3://{BUCKET}/{PROJECT}/Gold"

# -----------------------------------------------------
# Read Bronze Layer
# -----------------------------------------------------

customers_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{BRONZE}/customers.csv")
)

products_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{BRONZE}/products.csv")
)

orders_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{BRONZE}/orders.csv")
)

print("Bronze Layer Loaded Successfully")

print("Customers :", customers_df.count())
print("Products  :", products_df.count())
print("Orders    :", orders_df.count())

# -----------------------------------------------------
# Silver Layer
# Remove Duplicates
# -----------------------------------------------------

customers_silver = customers_df.dropDuplicates(["customer_id"])

products_silver = products_df.dropDuplicates(["product_id"])

orders_silver = orders_df.dropDuplicates(["order_id"])

# -----------------------------------------------------
# Remove Null Primary Keys
# -----------------------------------------------------

customers_silver = customers_silver.filter(
    col("customer_id").isNotNull()
)

products_silver = products_silver.filter(
    col("product_id").isNotNull()
)

orders_silver = orders_silver.filter(
    col("order_id").isNotNull()
)

# -----------------------------------------------------
# Business Rules
# -----------------------------------------------------

products_silver = products_silver.filter(
    col("price") > 0
)

orders_silver = orders_silver.filter(
    col("quantity") > 0
)

orders_silver = orders_silver.filter(
    col("order_amount") > 0
)

# -----------------------------------------------------
# Referential Integrity
# -----------------------------------------------------

orders_silver = (
    orders_silver
    .join(
        customers_silver.select("customer_id"),
        "customer_id",
        "inner"
    )
)

orders_silver = (
    orders_silver
    .join(
        products_silver.select("product_id"),
        "product_id",
        "inner"
    )
)

print("Silver Layer Validation Completed")

print("Customers :", customers_silver.count())
print("Products  :", products_silver.count())
print("Orders    :", orders_silver.count())

# -----------------------------------------------------
# Write Silver Layer
# -----------------------------------------------------

customers_silver.write.mode("overwrite").parquet(
    f"{SILVER}/customers"
)

products_silver.write.mode("overwrite").parquet(
    f"{SILVER}/products"
)

orders_silver.write.mode("overwrite").parquet(
    f"{SILVER}/orders"
)

print("Silver Layer Saved Successfully")

# =====================================================
# GOLD LAYER STARTS HERE
# (Continue with Part 2)
# =====================================================
# =====================================================
# GOLD LAYER
# =====================================================

print("Gold Layer Started")

# -----------------------------------------------------
# Customer Orders (Base DataFrame)
# -----------------------------------------------------

customer_orders = (
    orders_silver
    .join(
        customers_silver,
        "customer_id",
        "inner"
    )
)

# -----------------------------------------------------
# Customer Sales
# -----------------------------------------------------

customer_sales = (
    customer_orders
    .groupBy(
        "customer_id",
        "customer_name",
        "state"
    )
    .agg(
        count("order_id").alias("total_orders"),
        sum("quantity").alias("total_quantity"),
        sum("order_amount").alias("total_revenue"),
        avg("order_amount").alias("avg_order_value")
    )
)

print("Customer Sales Created")

# -----------------------------------------------------
# Product Sales
# -----------------------------------------------------

product_sales = (
    customer_orders
    .join(
        products_silver,
        "product_id",
        "inner"
    )
    .groupBy(
        "product_id",
        "product_name",
        "category"
    )
    .agg(
        sum("quantity").alias("total_quantity_sold"),
        sum("order_amount").alias("total_product_revenue")
    )
)

print("Product Sales Created")

# -----------------------------------------------------
# State Sales
# -----------------------------------------------------

state_sales = (
    customer_orders
    .groupBy("state")
    .agg(
        sum("order_amount").alias("total_state_revenue"),
        count("order_id").alias("total_orders")
    )
)

print("State Sales Created")

# -----------------------------------------------------
# Category Sales
# -----------------------------------------------------

category_sales = (
    customer_orders
    .join(
        products_silver,
        "product_id",
        "inner"
    )
    .groupBy("category")
    .agg(
        sum("order_amount").alias("total_category_revenue"),
        sum("quantity").alias("total_quantity")
    )
)

print("Category Sales Created")

# -----------------------------------------------------
# Top Customers
# -----------------------------------------------------

top_customers = (
    customer_sales
    .orderBy(
        desc("total_revenue")
    )
)

print("Top Customers Created")

# -----------------------------------------------------
# Top 3 Customers Per State
# -----------------------------------------------------

window_spec = Window.partitionBy(
    "state"
).orderBy(
    desc("total_revenue")
)

top3_customers_state = (
    customer_sales
    .withColumn(
        "rank",
        dense_rank().over(window_spec)
    )
    .filter(
        col("rank") <= 3
    )
)

print("Top 3 Customers Per State Created")

# -----------------------------------------------------
# KPI Dashboard Summary
# -----------------------------------------------------

kpi_summary = (
    orders_silver
    .agg(
        count("order_id").alias("total_orders"),
        sum("order_amount").alias("total_sales"),
        avg("order_amount").alias("average_order_value"),
        sum("quantity").alias("total_quantity")
    )
)

print("KPI Summary Created")

print("Gold Layer Completed Successfully")

# =====================================================
# WRITE GOLD LAYER STARTS HERE
# (Continue with Part 3)
# =====================================================
# =====================================================
# WRITE GOLD LAYER
# =====================================================

print("Writing Gold Layer to S3...")

customer_sales.write \
    .mode("overwrite") \
    .parquet(f"{GOLD}/Customer_sales")

print("✓ Customer Sales Saved")

product_sales.write \
    .mode("overwrite") \
    .parquet(f"{GOLD}/Product_sales")

print("✓ Product Sales Saved")

state_sales.write \
    .mode("overwrite") \
    .parquet(f"{GOLD}/State_sales")

print("✓ State Sales Saved")

category_sales.write \
    .mode("overwrite") \
    .parquet(f"{GOLD}/Category_sales")

print("✓ Category Sales Saved")

top_customers.write \
    .mode("overwrite") \
    .parquet(f"{GOLD}/Top_customers")

print("✓ Top Customers Saved")

top3_customers_state.write \
    .mode("overwrite") \
    .parquet(f"{GOLD}/Top3_Customers_Per_State")

print("✓ Top 3 Customers Per State Saved")

kpi_summary.write \
    .mode("overwrite") \
    .parquet(f"{GOLD}/KPI_Summary")

print("✓ KPI Summary Saved")

print("===================================================")
print("      RETAIL DATA ENGINEERING PIPELINE SUCCESS")
print("===================================================")

print("Customers :", customers_silver.count())
print("Products  :", products_silver.count())
print("Orders    :", orders_silver.count())

print("Customer Sales :", customer_sales.count())
print("Product Sales  :", product_sales.count())
print("State Sales    :", state_sales.count())
print("Category Sales :", category_sales.count())
print("Top Customers  :", top_customers.count())

print("===================================================")
print("Writing Completed Successfully")
print("===================================================")

# -----------------------------------------------------
# Commit Glue Job
# -----------------------------------------------------

job.commit()

print("Glue Job Completed Successfully")
