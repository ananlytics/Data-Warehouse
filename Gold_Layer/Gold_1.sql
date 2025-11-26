/***********************************************************************
  Gold Layer – Business-Ready Dimensional Model (Star Schema Views)
  Purpose : Create clean, enriched, analytics-ready views in the gold schema
  Source  : Silver layer tables
  Model   : Kimball-style dimensions + fact
***********************************************************************/

SET NOCOUNT ON;
GO

-- ===================================================================
-- Dimension: Customers (Enriched from CRM + ERP)
-- ===================================================================
IF EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID('gold.dim_customers'))
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY ci.cst_id ASC)                          AS CustomerSK,          -- Surrogate Key
    ci.cst_id                                                           AS CustomerID,          -- Natural Key from CRM
    ci.cst_key                                                          AS CustomerNumber,      -- Business-facing customer code
    ci.cst_firstname                                                    AS FirstName,
    ci.cst_lastname                                                     AS LastName,
    COALESCE(la.cntry, 'Unknown')                                       AS Country,
    ci.cst_marital_status                                               AS MaritalStatus,
    CASE 
        WHEN NULLIF(TRIM(ci.cst_gndr), 'n/a') IS NOT NULL THEN TRIM(ci.cst_gndr)
        ELSE COALESCE(NULLIF(TRIM(ca.gen), ''), 'Unknown')
    END                                                                 AS Gender,
    ca.bdate                                                            AS BirthDate,
    ci.cst_create_date                                                  AS CustomerSinceDate,
    GETDATE()                                                           AS LoadDate             -- Metadata
FROM silver.crm_cust_info      AS ci
LEFT JOIN silver.erp_cust_az12 AS ca  ON ca.cid  = ci.cst_key
LEFT JOIN silver.erp_loc_a101  AS la  ON la.cid  = ci.cst_key;
GO


-- ===================================================================
-- Dimension: Products (Current version only – Type 2 SCD filter)
-- ===================================================================
DROP VIEW IF EXISTS gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT
