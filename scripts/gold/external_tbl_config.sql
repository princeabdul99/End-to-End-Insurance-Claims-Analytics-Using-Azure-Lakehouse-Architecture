/*
===============================================================================
EXTERNAL TABLE Script: Create Gold Tables
===============================================================================
Script Purpose:
    This script creates table for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each table permanently stores the views data. Views data are temporarily stored and can be lost.

Usage:
    - These tables can be accessed later and can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Master Key
-- ============================================================================
CREATE MASTER KEY ENCRYPTION BY PASSWORD ='<your-password>';
GO

-- =============================================================================
-- Create Database Scoped Credential
-- ============================================================================
CREATE DATABASE SCOPED CREDENTIAL insurance_claims_cred
WITH
    IDENTITY = 'Managed Identity';
GO

-- =============================================================================
-- DATA SOURCE: Create External Data Source 
-- ============================================================================

IF EXISTS (
    SELECT * 
    FROM sys.external_data_sources 
    WHERE name = 'source_silver'
)
DROP EXTERNAL DATA SOURCE source_silver;
GO

CREATE EXTERNAL DATA SOURCE source_silver
WITH (
    LOCATION = 'https://insuranceclaimsdatalake.blob.core.windows.net/silver/',
    CREDENTIAL = insurance_claims_cred
);
GO

-- Drop source_gold if it exists
IF EXISTS (
    SELECT * 
    FROM sys.external_data_sources 
    WHERE name = 'source_gold'
)
DROP EXTERNAL DATA SOURCE source_gold;
GO

CREATE EXTERNAL DATA SOURCE source_gold
WITH (
    LOCATION = 'https://insuranceclaimsdatalake.blob.core.windows.net/gold/',
    CREDENTIAL = insurance_claims_cred
);
GO


-- =============================================================================
-- FILE FORMAT: Create External File Format
-- ============================================================================

CREATE EXTERNAL FILE FORMAT format_parquet
WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);
GO















