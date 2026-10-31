# Azure-End-to-End-Project-Adventure-Work-Dataset

## 📌 Project Overview
This repository contains the complete source code, configuration templates, and architectural patterns for an enterprise End-to-End Azure Data Engineering Platform built on the AdventureWorks dataset[cite: 23, 24]. 

Operating on a cloud-native **Medallion Architecture (Bronze ➔ Silver ➔ Gold)**, the pipeline automates data flows from external HTTP/API endpoints to executive Power BI dashboards[cite: 24]:
* **Ingestion (Bronze)**: Metadata-driven dynamic orchestration with Azure Data Factory and ADLS Gen2[cite: 24].
* **Transformation (Silver)**: Scalable PySpark batch transformations, multi-year unions, date formatting, and Parquet persistence with Azure Databricks[cite: 24].
* **Serving (Gold)**: Serverless SQL modeling using OPENROWSET views and CETAS external tables in Azure Synapse Analytics[cite: 22, 24].
* **Consumption**: Direct Synapse SQL endpoint connectivity for interactive Power BI KPI reporting[cite: 24].
# 🏢 Azure End-to-End Enterprise Lakehouse Platform (AdventureWorks)

An enterprise-grade, cloud-native data engineering solution built on Microsoft Azure. This project implements a fully automated **Medallion Architecture (Bronze ➔ Silver ➔ Gold)** to ingest retail sales data dynamically, clean and standardize transactional records at scale using PySpark, model logical views and external tables in Azure Synapse Serverless SQL, and deliver self-service analytics to Power BI.

---

## 🏛️ Architecture & End-to-End Flow

![Architecture Diagram](docs/architecture-diagram.png)

1. **Ingest (Bronze Layer)**:
   * **Azure Data Factory (ADF)** connects to GitHub HTTP API endpoints.
   * Leveraged dynamic parameterized pipelines (`Lookup` + `ForEach`) to ingest multi-table CSV datasets (`Calendar`, `Customers`, `Products`, `Sales 2015–2017`, `Returns`, `Territories`) into an **ADLS Gen2 Bronze** storage container.
2. **Transform (Silver Layer)**:
   * **Azure Databricks (PySpark)** reads Bronze datasets securely via Microsoft Entra ID OAuth 2.0 and Azure Key Vault secret scopes.
   * Multi-year sales transactions (`2015`, `2016`, `2017`) unified using `unionByName`.
   * Standardized schemas: parsed dates (`to_date`, `to_timestamp`), cast strict types (`IntegerType`), extracted temporal dimensions (`OrderYear`, `OrderMonth`), handled composite deduplication, and applied string splitting.
   * Written to the **ADLS Gen2 Silver** container in optimized columnar **Parquet** format.
3. **Serve (Gold Layer - Lakehouse / Logical DW)**:
   * Configured **Azure Synapse Analytics (Serverless SQL Pool)** linked via System-Assigned Managed Identity (`Storage Blob Data Contributor`).
   * Built abstracted SQL views using `OPENROWSET(BULK '...', FORMAT = 'PARQUET')` directly over Silver data lake files.
   * Implemented **CETAS (Create External Table As Select)** with Snappy-compressed Parquet to materialize high-performance analytical tables into the **ADLS Gen2 Gold** container.
4. **Consume (Reporting & BI)**:
   * Connected **Microsoft Power BI Desktop** directly to the Azure Synapse Serverless SQL endpoint (`ondemand.sql.azuresynapse.net`).
   * Visualized order volumes, customer retention metrics, product distributions, and historical sales trends.

---

## 🛠️ Technology Stack

| Layer | Service / Technology | Implementation Details |
|---|---|---|
| **Cloud Infrastructure** | Microsoft Azure | Resource Groups, IAM, RBAC, Managed Identities |
| **Data Lake** | ADLS Gen2 | Hierarchical Namespace enabled (`bronze`, `silver`, `gold`, `parameters`) |
| **Orchestration** | Azure Data Factory (ADF) | Dynamic parameterization, `Lookup`, `ForEach`, HTTP & ADLS Linked Services |
| **Compute & ETL** | Azure Databricks (Apache Spark 3.5) | Classic Single-Node Cluster, PySpark DataFrame API, Parquet writes |
| **Data Warehouse** | Azure Synapse Analytics | Serverless SQL Pools, `OPENROWSET`, Database-Scoped Credentials, CETAS |
| **Business Intelligence** | Microsoft Power BI | Direct Synapse SQL Endpoint connector, DAX modeling, KPI cards |

---

## ⚙️ Key Technical Highlights

### 1. Dynamic Parameterized Orchestration in ADF
Rather than creating repetitive static pipelines for 10+ tables, the pipeline leverages a metadata-driven approach:
* Ingests a control JSON configuration (`get.json`) using a `Lookup` activity.
* Iterates through relative endpoints, destination folders, and filenames dynamically within a single `ForEach` copy activity container.

### 2. PySpark Data Standardization (Databricks)
* **Schema Integrity**: Enforced strict casting on primary/foreign keys (`ProductKey`, `CustomerKey`, `TerritoryKey`).
* **Multi-Format Temporal Normalization**: Handled mismatched date formats (`MM-dd-yyyy` vs `M/d/yyyy`) into standard date and timestamp data types.
* **Deduplication**: Applied composite business key deduplication on `["OrderNumber", "OrderLineItem"]`.

### 3. Serverless SQL Modeling (Azure Synapse Analytics)
* **Zero Storage Duplication**: Built logical views using Synapse's serverless `OPENROWSET` query engine to read directly from ADLS Gen2 Silver Parquet files without running a persistent database instance.
* **Persistent Materialization via CETAS**:
  ```sql
  CREATE EXTERNAL TABLE gold.ext_sales
  WITH (
      LOCATION = 'external_sales/',
      DATA_SOURCE = source_gold,
      FILE_FORMAT = format_parquet
  )
  AS
  SELECT * FROM gold.sales;
