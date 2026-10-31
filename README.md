# Azure-End-to-End-Project-Adventure-Work-Dataset

# 🏢 Azure End-to-End Enterprise Data Engineering Project (AdventureWorks)

An enterprise-grade, cloud-native data engineering solution built on Microsoft Azure. This project implements a fully automated **Medallion Architecture (Bronze ➔ Silver ➔ Gold)** to ingest retail sales data dynamically, clean and standardize transactional records at scale using PySpark, model logical views and external tables in Azure Synapse Serverless SQL, and deliver self-service analytics to Power BI.

---

## 🏛️ Architecture & Data Flow

Below is the complete architectural layout of the pipeline, showcasing the dynamic transition of data through ingestion, transformation, serving, and visualization layers:

<p align="center">
  <img src="docs/architecture.png" alt="Azure Medallion Architecture Flowchart" width="100%">
</p>

1. **Ingest (Bronze)**: Metadata-driven dynamic ingestion with Azure Data Factory into ADLS Gen2.
2. **Transform (Silver)**: PySpark batch transformation and standardization inside Azure Databricks.
3. **Serve (Gold)**: Abstracted serverless views and optimized CETAS table materialization with Azure Synapse Analytics.
4. **Consume (Reporting)**: KPI dashboard delivery via Direct Synapse SQL endpoint connector in Power BI.

---

## ⚙️ Layer-by-Layer Implementation & Screenshots

### 1️⃣ Data Ingestion & Storage (ADF & ADLS Gen2 Bronze)
* **Pipeline Configuration**: Leveraged a metadata-driven dynamic framework to replace static copy tasks. By ingesting a control parameters file, the ADF `ForEach` container dynamically loops through 11 operational targets to run parameterized copies.

<p align="center">
  <img src="docs/adf_pipeline.png" alt="Azure Data Factory Dynamic Ingestion Pipeline" width="90%">
</p>

* **Data Storage**: Data lands in its raw, unaltered state in ADLS Gen2 `bronze` container organized dynamically by entities.

<p align="center">
  <img src="docs/adls_bronze.png" alt="Azure Data Lake ADLS Gen2 Bronze Container Store" width="90%">
</p>

#### Data Ingested (GitHub Data API Source)
* `AdventureWorks_Calendar`
* `AdventureWorks_Customers`
* `AdventureWorks_Product_Categories`
* `AdventureWorks_Product_Subcategories`
* `AdventureWorks_Products`
* `AdventureWorks_Returns`
* `AdventureWorks_Sales_2015 / 2016 / 2017`
* `AdventureWorks_Territories`

---

### 2️⃣ PySpark Transformation (Azure Databricks Silver)
* Authenticated securely to ADLS Gen2 utilizing OAuth 2.0 through Microsoft Entra ID App registrations, keeping parameters safe with Azure Key Vault secrets.
* Applied massive transform operations: Unified multiple years of transactional partition files (`Sales 2015`, `2016`, `2017`), parsed irregular date formats (`MM-dd-yyyy` and `M/d/yyyy`), cast strict schema typing, handled nulls, split string IDs (`split`), and deleted composite duplicates.
* Persisted datasets as partitioned Parquet files inside the `silver` storage zone.

```python
# Spark 3.5.0 / PySpark Transform Sample for Sales partition consolidation:

# 1. Read all 3 years dynamically
sales_2015 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(f"abfss://{container_bronze}@{storage_account}.dfs.core.windows.net/AdventureWorks_Sales_2015*")
sales_2016 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(f"abfss://{container_bronze}@{storage_account}.dfs.core.windows.net/AdventureWorks_Sales_2016*")
sales_2017 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(f"abfss://{container_bronze}@{storage_account}.dfs.core.windows.net/AdventureWorks_Sales_2017*")

# 2. Combine years and parse schemas
df_sales_raw = sales_2015.unionByName(sales_2016).unionByName(sales_2017)

df_sales_clean = (df_sales_raw
    .withColumn("OrderDate", to_date(col("OrderDate"), "MM-dd-yyyy"))
    .withColumn("StockDate", to_date(col("StockDate"), "M/d/yyyy"))
    .withColumn("CustomerKey", col("CustomerKey").cast(IntegerType()))
    .withColumn("OrderYear", year(col("OrderDate")))
    .withColumn("OrderMonth", month(col("OrderDate")))
    .dropna(subset=["OrderNumber", "OrderDate", "ProductKey"])
    .dropDuplicates(["OrderNumber", "OrderLineItem"])
)

# 3. Write to Silver container in Parquet format
df_sales_clean.write.format("parquet").mode("overwrite").save(f"abfss://silver@{storage_account}.dfs.core.windows.net/AdventureWorks_Sales/")
