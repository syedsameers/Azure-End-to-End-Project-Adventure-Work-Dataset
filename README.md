# Azure-End-to-End-Project-Adventure-Work-Dataset

# 🏢 Azure End-to-End Enterprise Data Engineering Project (AdventureWorks)

An enterprise-grade, cloud-native data engineering solution built on Microsoft Azure. This project implements a fully automated **Medallion Architecture (Bronze ➔ Silver ➔ Gold)** to ingest retail sales data dynamically, clean and standardize transactional records at scale using PySpark, model logical views and external tables in Azure Synapse Serverless SQL, and deliver self-service analytics to Power BI.

---

## 🏛️ Architecture & Data Flow

Below is the complete architectural layout of the pipeline, showcasing the dynamic transition of data through ingestion, transformation, serving, and visualization layers:

<p align="center">
  <img src="architecture.png" alt="Azure Medallion Architecture Flowchart" width="100%">
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
  <img src="adf_pipeline.png" alt="Azure Data Factory Dynamic Ingestion Pipeline" width="90%">
</p>

* **Data Storage**: Data lands in its raw, unaltered state in ADLS Gen2 `bronze` container organized dynamically by entities.

<p align="center">
  <img src="adls_bronze.png" alt="Azure Data Lake ADLS Gen2 Bronze Container Store" width="90%">
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

<p align="center">
  <img src="databricks_pyspark.png" alt="Azure Databricks PySpark Notebook Silver Layer" width="90%">
</p>

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

---

### 3️⃣ Serving Layer & Logical Data Warehouse (Azure Synapse Analytics Gold)

To make data ready for analytical queries without duplicating storage or managing expensive always-on SQL databases, **Azure Synapse Analytics Serverless SQL Pools** was used as the Gold serving layer.

#### Key Architectural Implementations:
1. **Managed Identity Security**: Configured Azure Synapse workspace with a System-Assigned Managed Identity and granted it `Storage Blob Data Contributor` on the ADLS Gen2 storage account to ensure zero credential leaks.
2. **Serverless Abstraction Layer (`OPENROWSET`)**:
   * Created serverless logical views over cleansed Silver Parquet files directly within the data lake.
   * Enables ad-hoc SQL querying without loading raw data into persistent relational storage, reducing compute and infrastructure costs.
3. **Optimized Materialization via CETAS**:
   * Implemented **CETAS (Create External Table As Select)** to generate optimized analytical Gold tables saved back into the `gold` container with Snappy Parquet compression.

#### SQL Implementation Scripts:

```sql
-- 1. Create Dedicated Analytical Schema
CREATE SCHEMA gold;
GO

-- 2. Create Serverless Views Using OPENROWSET Over Silver Parquet Files
CREATE VIEW gold.calendar
AS
SELECT *
FROM OPENROWSET(
    BULK '[https://azuree2eprojectdatalake.dfs.core.windows.net/silver/AdventureWorks_Calendar/](https://azuree2eprojectdatalake.dfs.core.windows.net/silver/AdventureWorks_Calendar/)',
    FORMAT = 'PARQUET'
) AS QUERY1;
GO

CREATE VIEW gold.customers
AS
SELECT *
FROM OPENROWSET(
    BULK '[https://azuree2eprojectdatalake.dfs.core.windows.net/silver/AdventureWorks_Customers/](https://azuree2eprojectdatalake.dfs.core.windows.net/silver/AdventureWorks_Customers/)',
    FORMAT = 'PARQUET'
) AS QUERY1;
GO

CREATE VIEW gold.products
AS
SELECT *
FROM OPENROWSET(
    BULK '[https://azuree2eprojectdatalake.dfs.core.windows.net/silver/AdventureWorks_Products/](https://azuree2eprojectdatalake.dfs.core.windows.net/silver/AdventureWorks_Products/)',
    FORMAT = 'PARQUET'
) AS QUERY1;
GO

-- 3. Materialize Gold Tables Using CETAS (Create External Table As Select)
CREATE EXTERNAL TABLE gold.ext_sales
WITH (
    LOCATION = 'external_sales/',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * 
FROM gold.sales;
GO

<p align="center">
  <img src="synapse_gold.png.png" alt="Azure Synapse Analytics Serverless SQL Views Creation" width="90%">
</p>

4️⃣ Business Intelligence & Visual Analytics (Microsoft Power BI)
The final phase bridges enterprise data engineering with business decision-making by delivering self-service business intelligence dashboards directly connected to the Synapse serverless SQL endpoint.

Implementation & Architecture:
Connectivity: Connected Power BI Desktop to Azure Synapse using the native Azure Synapse Analytics SQL connector via the workspace Serverless SQL Endpoint (<workspace-name>-ondemand.sql.azuresynapse.net).

Semantic Modeling: Modeled star-schema analytical relationships between the materialized gold.ext_sales fact table and dimension views (customers, products, calendar, territories).

Core Business KPIs & Visualizations:

Executive Cards: Dynamic tracking of total active customers, gross order quantities, and product catalog volumes.

Order Progression Curve: Area chart evaluating historical order volumes across financial quarters and years (2015–2017).

Order Quantity vs. Line Item Movement: Visualizing product batch movement and high-frequency purchase thresholds.
<p align="center">
  <img src="powerbi_dashboard.png" alt="AdventureWorks Sales Power BI Dashboard" width="90%">
</p>
