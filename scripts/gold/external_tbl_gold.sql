-- =============================================================================
-- CREATE EXTERNAL TABLES
-- ============================================================================


-- =============================================================================
-- Table : gold.dim_state_region
-- ============================================================================
IF OBJECT_ID('gold.dim_state_region', 'U') IS NOT NULL
	DROP EXTERNAL TABLE gold.dim_state_region;
GO

CREATE EXTERNAL TABLE gold.dim_state_region
WITH (
    LOCATION = 'state-region',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
) 
AS 
SELECT * FROM gold.dim_state_regions;
GO

-- =============================================================================
-- Table : gold.dim_region
-- ============================================================================
IF OBJECT_ID('gold.dim_region', 'U') IS NOT NULL
	DROP EXTERNAL TABLE gold.dim_region;
GO

CREATE EXTERNAL TABLE gold.dim_region
WITH (
    LOCATION = 'region',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
) 
AS 
SELECT * FROM gold.dim_regions;
GO

-- =============================================================================
-- Table : gold.dim_broker
-- ============================================================================
IF OBJECT_ID('gold.dim_broker', 'U') IS NOT NULL
	DROP EXTERNAL TABLE gold.dim_broker;
GO

CREATE EXTERNAL TABLE gold.dim_broker
WITH (
    LOCATION = 'broker',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
) 
AS 
SELECT * FROM gold.dim_brokers;
GO

-- =============================================================================
-- Table : gold.dim_coverage
-- ============================================================================
IF OBJECT_ID('gold.dim_coverage', 'U') IS NOT NULL
	DROP EXTERNAL TABLE gold.dim_coverage;
GO

CREATE EXTERNAL TABLE gold.dim_coverage
WITH (
    LOCATION = 'coverage',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
) 
AS 
SELECT * FROM gold.dim_coverages;
GO

-- =============================================================================
-- Table : gold.dim_participant
-- ============================================================================
IF OBJECT_ID('gold.dim_participant', 'U') IS NOT NULL
	DROP EXTERNAL TABLE gold.dim_participant;
GO

CREATE EXTERNAL TABLE gold.dim_participant
WITH (
    LOCATION = 'participant',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
) 
AS 
SELECT * FROM gold.dim_participants;
GO

-- =============================================================================
-- Table : gold.dim_product
-- ============================================================================
IF OBJECT_ID('gold.dim_product', 'U') IS NOT NULL
	DROP EXTERNAL TABLE gold.dim_product;
GO

CREATE EXTERNAL TABLE gold.dim_product
WITH (
    LOCATION = 'product',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
) 
AS 
SELECT * FROM gold.dim_products;
GO

-- =============================================================================
-- Table : gold.dim_policy
-- ============================================================================
IF OBJECT_ID('gold.dim_policy', 'U') IS NOT NULL
	DROP EXTERNAL TABLE gold.dim_policy;
GO

CREATE EXTERNAL TABLE gold.dim_policy
WITH (
    LOCATION = 'policy',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
) 
AS 
SELECT * FROM gold.dim_policies;
GO

-- =============================================================================
-- Table : gold.fact_claims_announcement
-- ============================================================================
IF OBJECT_ID('gold.fact_claims_announcement', 'U') IS NOT NULL
	DROP EXTERNAL TABLE gold.fact_claims_announcement;
GO

CREATE EXTERNAL TABLE gold.fact_claims_announcement
WITH (
    LOCATION = 'claims-announcement',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
) 
AS 
SELECT * FROM gold.fact_claims_announcements;
GO

-- =============================================================================
-- Table : gold.fact_claims_reserve
-- ============================================================================
IF OBJECT_ID('gold.fact_claims_reserve', 'U') IS NOT NULL
	DROP EXTERNAL TABLE gold.fact_claims_reserve;
GO

CREATE EXTERNAL TABLE gold.fact_claims_reserve
WITH (
    LOCATION = 'claims-reserve',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
) 
AS 
SELECT * FROM gold.fact_claims_reserves;
GO


-- =============================================================================
-- Table : gold.fact_claims_payment
-- ============================================================================
IF OBJECT_ID('gold.fact_claims_payment', 'U') IS NOT NULL
	DROP EXTERNAL TABLE gold.fact_claims_payment;
GO

CREATE EXTERNAL TABLE gold.fact_claims_payment
WITH (
    LOCATION = 'claims-payment',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
) 
AS 
SELECT * FROM gold.fact_claims_payments;
GO












