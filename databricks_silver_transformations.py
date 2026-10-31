# Databricks Notebook source
# ==============================================================================
# Pipeline: AdventureWorks End-to-End Lakehouse Platform
# Layer: Bronze to Silver Transformations
# Compute: Azure Databricks (Apache Spark 3.5.0)
# ==============================================================================

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Storage Authentication Configuration (Entra ID OAuth 2.0)

# COMMAND ----------
# Storage Account Details
storage_account_name = "azuree2eprojectdatalake"
container_bronze = "bronze"
container_silver = "silver"

# Microsoft Entra ID (Service Principal Credentials)
# In production, fetch via dbutils.secrets.get(scope="key-vault-scope", key="...")
client_id = "-"
client_secret = "-"
tenant_id = ""

# Register Hadoop Azure Blob File System (ABFS) Configurations
spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# Base paths
bronze_base = f"abfss://{container_bronze}@{storage_account_name}.dfs.core.windows.net"
silver_base = f"abfss://{container_silver}@{storage_account_name}.dfs.core.windows.net"

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Imports

# COMMAND ----------
from pyspark.sql.functions import col, to_date, year, month, split, to_timestamp, regexp_replace
from pyspark.sql.types import IntegerType

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Calendar Data Transformation

# COMMAND ----------
# Read Calendar from Bronze
df_calendar = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f"{bronze_base}/AdventureWorks_Calendar*")

# Transform Date Dimensions
df_calendar_clean = df_calendar \
    .withColumn("Date", to_date(col("Date"), "M/d/yyyy")) \
    .withColumn("Month", month(col("Date"))) \
    .withColumn("Year", year(col("Date")))

# Write to Silver Layer
df_calendar_clean.write.format("parquet") \
    .mode("overwrite") \
    .save(f"{silver_base}/AdventureWorks_Calendar/")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Customers Data Transformation

# COMMAND ----------
df_customers = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f"{bronze_base}/AdventureWorks_Customers*")

# Concatenate full name and normalize
df_customers_clean = df_customers \
    .withColumn("BirthDate", to_date(col("BirthDate"), "M/d/yyyy"))

df_customers_clean.write.format("parquet") \
    .mode("overwrite") \
    .save(f"{silver_base}/AdventureWorks_Customers/")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Products & Categories Transformations

# COMMAND ----------
# Products
df_products = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f"{bronze_base}/AdventureWorks_Products*")

df_products_clean = df_products \
    .withColumn("ProductSKU", split(col("ProductSKU"), "-").getItem(0)) \
    .withColumn("ProductName", split(col("ProductName"), " ").getItem(0))

df_products_clean.write.format("parquet") \
    .mode("overwrite") \
    .save(f"{silver_base}/AdventureWorks_Products/")

# Product Categories
df_categories = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f"{bronze_base}/AdventureWorks_Product_Categories*")

df_categories.write.format("parquet") \
    .mode("overwrite") \
    .save(f"{silver_base}/AdventureWorks_Product_Categories/")

# Product Subcategories
df_subcategories = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f"{bronze_base}/AdventureWorks_Product_Subcategories*")

df_subcategories.write.format("parquet") \
    .mode("overwrite") \
    .save(f"{silver_base}/AdventureWorks_Product_Subcategories/")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Multi-Year Sales Consolidation & Transformation (2015–2017)

# COMMAND ----------
# Read all 3 partitions dynamically
sales_2015 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(f"{bronze_base}/AdventureWorks_Sales_2015*")
sales_2016 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(f"{bronze_base}/AdventureWorks_Sales_2016*")
sales_2017 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(f"{bronze_base}/AdventureWorks_Sales_2017*")

# Union partitions
df_sales_raw = sales_2015.unionByName(sales_2016).unionByName(sales_2017)

# Clean schema, parse multi-format dates, cast types, and deduplicate
df_sales_clean = (df_sales_raw
    .withColumn("OrderDate", to_date(col("OrderDate"), "MM-dd-yyyy"))
    .withColumn("StockDate", to_date(col("StockDate"), "M/d/yyyy"))
    .withColumn("ProductKey", col("ProductKey").cast(IntegerType()))
    .withColumn("CustomerKey", col("CustomerKey").cast(IntegerType()))
    .withColumn("TerritoryKey", col("TerritoryKey").cast(IntegerType()))
    .withColumn("OrderLineItem", col("OrderLineItem").cast(IntegerType()))
    .withColumn("OrderQuantity", col("OrderQuantity").cast(IntegerType()))
    .withColumn("OrderYear", year(col("OrderDate")))
    .withColumn("OrderMonth", month(col("OrderDate")))
    .dropna(subset=["OrderNumber", "OrderDate", "ProductKey"])
    .dropDuplicates(["OrderNumber", "OrderLineItem"])
)

# Persist to Silver as Parquet
df_sales_clean.write.format("parquet") \
    .mode("overwrite") \
    .save(f"{silver_base}/AdventureWorks_Sales/")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Returns & Territories

# COMMAND ----------
# Returns
df_returns = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f"{bronze_base}/AdventureWorks_Returns*")

df_returns.write.format("parquet") \
    .mode("overwrite") \
    .save(f"{silver_base}/AdventureWorks_Returns/")

# Territories
df_territories = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f"{bronze_base}/AdventureWorks_Territories*")

df_territories.write.format("parquet") \
    .mode("overwrite") \
    .save(f"{silver_base}/AdventureWorks_Territories/")
