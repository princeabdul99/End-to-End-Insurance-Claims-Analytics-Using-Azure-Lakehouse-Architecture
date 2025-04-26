/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/


-- =============================================================================
-- Create Dimension: gold.dim_state_regions
-- =============================================================================
IF OBJECT_ID('gold.dim_state_regions', 'V') IS NOT NULL
	DROP VIEW gold.dim_state_regions;
GO

CREATE VIEW gold.dim_state_regions AS
SELECT 
    st.state_code,
    st.state,
    st.region
    FROM 
        OPENROWSET(
           BULK 'https://insuranceclaimsdatalake.blob.core.windows.net/silver/state-regions/',
           FORMAT = 'PARQUET'
        ) as st 
GO


-- =============================================================================
-- Create Dimension: gold.dim_regions
-- =============================================================================
IF OBJECT_ID('gold.dim_regions', 'V') IS NOT NULL
	DROP VIEW gold.dim_regions;
GO

CREATE VIEW gold.dim_regions AS
SELECT 
    r.id as region_id,
    r.name,
    r.county,
    st.state_code,
    r.state,
    r.type,
    r.latitude,
    r.longitude, 
    r.area_code,
    r.population,
    r.households,
    r.median_income,
    r.land_area,
    r.water_area,
    r.time_zone
    FROM 
        OPENROWSET(
           BULK 'https://insuranceclaimsdatalake.blob.core.windows.net/silver/regions/',
           FORMAT = 'PARQUET'
        ) as r

    LEFT JOIN gold.dim_state_regions AS st
        ON r.state_code = st.state_code;    
GO


-- =============================================================================
-- Create Dimension: gold.dim_brokers
-- =============================================================================
IF OBJECT_ID('gold.dim_brokers', 'V') IS NOT NULL
	DROP VIEW gold.dim_brokers;
GO

CREATE VIEW gold.dim_brokers AS
SELECT 
    br.broker_id,
    br.broker_code,
    br.broker_fullname,
    br.distribution_network,
    br.distribution_channel,
    br.commission_scheme
    FROM 
        OPENROWSET(
           BULK 'https://insuranceclaimsdatalake.blob.core.windows.net/silver/brokers/',
           FORMAT = 'PARQUET'
        ) as br
GO


-- =============================================================================
-- Create Dimension: gold.dim_coverages
-- =============================================================================
IF OBJECT_ID('gold.dim_coverages', 'V') IS NOT NULL
	DROP VIEW gold.dim_coverages;
GO

CREATE VIEW gold.dim_coverages AS
SELECT 
    c.cover_id,
    c.cover_code,
    c.renewal_type,
    c.room,
    c.participation,
    c.product_category,
    c.premium_mode,
    c.product_distribution
    FROM 
        OPENROWSET(
           BULK 'https://insuranceclaimsdatalake.blob.core.windows.net/silver/coverages/',
           FORMAT = 'PARQUET'
        ) as c
GO



-- =============================================================================
-- Create Dimension: gold.dim_participants
-- =============================================================================
IF OBJECT_ID('gold.dim_participants', 'V') IS NOT NULL
	DROP VIEW gold.dim_participants;
GO

CREATE VIEW gold.dim_participants AS
SELECT 
    p.participant_id,
    p.participant_code,
    p.participant_type,
    r.region_id,
    p.lastname,
    p.firstname,
    p.fullname,
    p.birthdate,
    p.gender,
    p.marital_status
    

    FROM 
        OPENROWSET(
           BULK 'https://insuranceclaimsdatalake.blob.core.windows.net/silver/participants/',
           FORMAT = 'PARQUET'
        ) as p

    LEFT JOIN gold.dim_regions as r
        ON p.region_id = r.region_id    
GO




-- =============================================================================
-- Create Dimension: gold.dim_policies
-- =============================================================================
IF OBJECT_ID('gold.dim_policies', 'V') IS NOT NULL
	DROP VIEW gold.dim_policies;
GO

CREATE VIEW gold.dim_policies AS
SELECT 
    p.policy_id,
    p.policy_code,
    p.policy_inception_date as inception_date,
    p.cancelation_date,
    p.policy_start_date as start_date,
    p.policy_expiration_date as expiry_date,
    p.renewal_month,
    p.annualized_policy_premium,
    p.policy_status

    FROM 
        OPENROWSET(
           BULK 'https://insuranceclaimsdatalake.blob.core.windows.net/silver/policies/',
           FORMAT = 'PARQUET'
        ) as p
GO



-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
	DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT 
    p.product_id,
    p.product_category,
    p.product_sub_category,
    p.product
    FROM 
        OPENROWSET(
           BULK 'https://insuranceclaimsdatalake.blob.core.windows.net/silver/products/',
           FORMAT = 'PARQUET'
        ) as p
GO



-- =============================================================================
-- Create Fact Table: gold.fact_claims_announcements
-- =============================================================================
IF OBJECT_ID('gold.fact_claims_announcements', 'V') IS NOT NULL
	DROP VIEW gold.fact_claims_announcements;
GO

CREATE VIEW gold.fact_claims_announcements AS
SELECT 
    ca.claim_id,
    ca.claim_code,
    pr.policy_id,
    ca.policy_code,
    ca.announcement_date,
    ca.event_date,
    ca.closing_date,
    ca.last_forecast_amount,
    br.broker_id,
    ins.participant_id,
    pd.product_id


    FROM 
        OPENROWSET(
           BULK 'https://insuranceclaimsdatalake.blob.core.windows.net/silver/claims-announcement/',
           FORMAT = 'PARQUET'
        ) as ca

    LEFT JOIN gold.dim_policies pr
        ON ca.policy_id = pr.policy_id

    LEFT JOIN gold.dim_brokers br
        ON ca.broker_id = br.broker_id          

    LEFT JOIN gold.dim_participants ins
        ON ca.insured_id = ins.participant_id

    LEFT JOIN gold.dim_products pd
        ON ca.product_id = pd.product_id           
GO


-- =============================================================================
-- Create Fact Table: gold.fact_claims_reserves
-- =============================================================================
IF OBJECT_ID('gold.fact_claims_reserves', 'V') IS NOT NULL
	DROP VIEW gold.fact_claims_reserves;
GO

CREATE VIEW gold.fact_claims_reserves AS
SELECT 
    ca.claim_id,
    ca.claim_code,
    pr.policy_id,
    co.cover_id,
    ca.announcement_date,
    ca.closing_date,
    ca.provision_amount,
    ca.provision_date,
    br.broker_id,
    ins.participant_id,
    pd.product_id


    FROM 
        OPENROWSET(
           BULK 'https://insuranceclaimsdatalake.blob.core.windows.net/silver/claims-reserves/',
           FORMAT = 'PARQUET'
        ) as ca

    LEFT JOIN gold.dim_policies pr
        ON ca.policy_id = pr.policy_id

    LEFT JOIN gold.dim_brokers br
        ON ca.broker_id = br.broker_id          

    LEFT JOIN gold.dim_participants ins
        ON ca.insured_id = ins.participant_id

    LEFT JOIN gold.dim_products pd
        ON ca.product_id = pd.product_id

    LEFT JOIN gold.dim_coverages co
        ON ca.cover_id = co.cover_id                   
GO



-- =============================================================================
-- Create Fact Table: gold.fact_claims_payments
-- =============================================================================
IF OBJECT_ID('gold.fact_claims_payments', 'V') IS NOT NULL
	DROP VIEW gold.fact_claims_payments;
GO

CREATE VIEW gold.fact_claims_payments AS
SELECT 
    ca.payment_code,
    ca.claim_id,
    ca.claim_code,
    pr.policy_id,
    co.cover_id,
    ca.announcement_date,
    ca.event_date,
    ca.closing_date,
    ca.payment_date,
    ca.payment_amount,
    ca.payment_type,
    br.broker_id,
    ins.participant_id,
    pd.product_id
   
    FROM 
        OPENROWSET(
           BULK 'https://insuranceclaimsdatalake.blob.core.windows.net/silver/claims-payments/',
           FORMAT = 'PARQUET'
        ) as ca

    LEFT JOIN gold.dim_policies pr
        ON ca.policy_id = pr.policy_id

    LEFT JOIN gold.dim_coverages co
        ON ca.cover_id = co.cover_id      

    LEFT JOIN gold.dim_brokers br
        ON ca.broker_id = br.broker_id          

    LEFT JOIN gold.dim_participants ins
        ON ca.insured_id = ins.participant_id

    LEFT JOIN gold.dim_products pd
        ON ca.product_id = pd.product_id           
GO
