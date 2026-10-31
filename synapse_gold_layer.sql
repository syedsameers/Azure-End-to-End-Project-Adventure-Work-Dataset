-- ==============================================================================
-- Pipeline: AdventureWorks End-to-End Lakehouse Platform
-- Layer: Gold Serving Layer (Logical DW & External Tables)
-- Engine: Azure Synapse Analytics (Serverless SQL Pool)
-- ==============================================================================

USE awprojectdatabase;
GO

-- 1. Create Dedicated Analytical Schema
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
    EXEC('CREATE SCHEMA gold');
GO

-- 2. Create Serverless Views Over Silver Parquet Files (Zero-Copy Abstraction)
-- ------------------------------------------------------------------------------

-- View: Calendar
CREATE OR ALTER VIEW gold.calendar
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://azuree2eprojectdatalake.dfs.core.windows.net/silver/AdventureWorks_Calendar/',
    FORMAT = 'PARQUET'
) AS [result];
GO

-- View: Customers
CREATE OR ALTER VIEW gold.customers
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://azuree2eprojectdatalake.dfs.core.windows.net/silver/AdventureWorks_Customers/',
    FORMAT = 'PARQUET'
) AS [result];
GO

-- View: Products
CREATE OR ALTER VIEW gold.products
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://azuree2eprojectdatalake.dfs.core.windows.net/silver/AdventureWorks_Products/',
    FORMAT = 'PARQUET'
) AS [result];
GO

-- View: Product Categories
CREATE OR ALTER VIEW gold.categories
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://azuree2eprojectdatalake.dfs.core.windows.net/silver/AdventureWorks_Product_Categories/',
    FORMAT = 'PARQUET'
) AS [result];
GO

-- View: Product Subcategories
CREATE OR ALTER VIEW gold.productsubcategories
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://azuree2eprojectdatalake.dfs.core.windows.net/silver/AdventureWorks_Product_Subcategories/',
    FORMAT = 'PARQUET'
) AS [result];
GO

-- View: Returns
CREATE OR ALTER VIEW gold.returns
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://azuree2eprojectdatalake.dfs.core.windows.net/silver/AdventureWorks_Returns/',
    FORMAT = 'PARQUET'
) AS [result];
GO

-- View: Territories
CREATE OR ALTER VIEW gold.territories
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://azuree2eprojectdatalake.dfs.core.windows.net/silver/AdventureWorks_Territories/',
    FORMAT = 'PARQUET'
) AS [result];
GO

-- View: Sales (Consolidated 2015-2017)
CREATE OR ALTER VIEW gold.sales
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://azuree2eprojectdatalake.dfs.core.windows.net/silver/AdventureWorks_Sales/',
    FORMAT = 'PARQUET'
) AS [result];
GO

-- 3. External Table Setup (CETAS - Create External Table As Select)
-- ------------------------------------------------------------------------------

-- Create Database Scoped Credential using Managed Identity
IF NOT EXISTS (SELECT * FROM sys.database_scoped_credentials WHERE name = 'synapse_managed_identity')
    CREATE DATABASE SCOPED CREDENTIAL synapse_managed_identity
    WITH IDENTITY = 'Managed Identity';
GO

-- Create External Data Source for Gold Storage
IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'source_gold')
    CREATE EXTERNAL DATA SOURCE source_gold
    WITH (
        LOCATION = 'https://azuree2eprojectdatalake.dfs.core.windows.net/gold',
        CREDENTIAL = synapse_managed_identity
    );
GO

-- Create External File Format for Parquet
IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'format_parquet')
    CREATE EXTERNAL FILE FORMAT format_parquet
    WITH (
        FORMAT_TYPE = PARQUET,
        DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
    );
GO

-- Materialize External Sales Table into Gold Storage Container
IF OBJECT_ID('gold.ext_sales') IS NOT NULL
    DROP EXTERNAL TABLE gold.ext_sales;

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
