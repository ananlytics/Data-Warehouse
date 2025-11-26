/***********************************************************************
  Silver Layer – Cleaned & Standardized Tables (Medallion Architecture)
  Purpose : Define persistent silver schema with audit column + readable aliases
  Note    : Column aliases are added as PERSISTED computed columns for query convenience
***********************************************************************/

SET NOCOUNT ON;
GO

-- ===================================================================
-- Silver: CRM Customer Master (Enriched & Standardized)
-- ===================================================================
IF EXISTS (SELECT 1 FROM sys.tables WHERE object_id = OBJECT_ID(N'silver.crm_cust_info'))
    DROP TABLE silver.crm_cust_info;
GO

CREATE TABLE silver.crm_cust_info
(
    -- Core business columns
    cst_id              INT              NOT NULL,
    cst_key             NVARCHAR(50)     NOT NULL,
    cst_firstname       NVARCHAR(50)     NULL,
    cst_lastname        NVARCHAR(50)     NULL,
    cst_marital_status  NVARCHAR(50)     NULL,
    cst_gndr            NVARCHAR(50)     NULL,
    cst_create_date     DATE             NULL,

    -- Audit column
    dwh_create_date     DATETIME2(7)     CONSTRAINT DF_silver_crm_cust_info_dwh_create_date 
                                         DEFAULT (SYSDATETIME()) NOT NULL,

    -- Meaningful aliases as persisted computed columns
    CustomerID          AS cst_id               PERSISTED,
    CustomerKey         AS cst_key              PERSISTED,
    FirstName           AS cst_firstname        PERSISTED,
    LastName            AS cst_lastname         PERSISTED,
    MaritalStatus       AS cst_marital_status   PERSISTED,
    Gender              AS cst_gndr             PERSISTED,
    CustomerSince       AS cst_create_date      PERSISTED
);
GO


-- ===================================================================
-- Silver: CRM Product Master (with SCD awareness)
-- ===================================================================
DROP TABLE IF EXISTS silver.crm_prd_info;
GO

CREATE TABLE silver.crm_prd_info
(
    prd_id         INT            NOT NULL,
    cat_id         NVARCHAR(50)   NULL,
    prd_key        NVARCHAR(50)   NOT NULL,
    prd_nm         NVARCHAR(50)   NULL,
    prd_cost       INT            NULL,
    prd_line       NVARCHAR(50)   NULL,
    prd_start_dt   DATE           NULL,
    prd_end_dt     DATE           NULL,
    dwh_create_date DATETIME2(7)  DEFAULT (SYSDATETIME()) NOT NULL,

    -- Clean aliases
    ProductID      AS prd_id         PERSISTED,
    CategoryID     AS cat_id         PERSISTED,
    ProductKey     AS prd_key        PERSISTED,
    ProductName    AS prd_nm         PERSISTED,
    UnitCost       AS prd_cost       PERSISTED,
    ProductLine    AS prd_line       PERSISTED,
    ValidFrom      AS prd_start_dt   PERSISTED,
    ValidTo        AS prd_end_dt     PERSISTED
);
GO


-- ===================================================================
-- Silver: CRM Sales Transactions
-- ===================================================================
IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
GO

CREATE TABLE silver.crm_sales_details
(
    sls_ord_num     NVARCHAR(50)   NOT NULL,
    sls_prd_key     NVARCHAR(50)   NOT NULL,
    sls_cust_id     INT            NOT NULL,
    sls_order_dt    DATE           NULL,
    sls_ship_dt     DATE           NULL,
    sls_due_dt      DATE           NULL,
    sls_sales       DECIMAL(12,2)  NULL,   -- Changed to DECIMAL for accuracy
    sls_quantity    INT            NULL,
    sls_price       DECIMAL(10,2)  NULL,
    dwh_create_date DATETIME2(7)   DEFAULT (SYSDATETIME()) NOT NULL,

    -- User-friendly aliases
    OrderNumber     AS sls_ord_num     PERSISTED,
    ProductKey      AS sls_prd_key     PERSISTED,
    CustomerID      AS sls_cust_id     PERSISTED,
    OrderDate       AS sls_order_dt    PERSISTED,
    ShipDate        AS sls_ship_dt     PERSISTED,
    DueDate         AS sls_due_dt      PERSISTED,
    SalesAmount     AS sls_sales       PERSISTED,
    Quantity        AS sls_quantity    PERSISTED,
    UnitPrice       AS sls_price       PERSISTED
);
GO


-- ===================================================================
-- Silver: ERP Location Dimension (A101)
-- ===================================================================
DROP TABLE IF EXISTS silver.erp_loc_a101;
GO

CREATE TABLE silver.erp_loc_a101
(
    cid             NVARCHAR(50)   NOT NULL,
    cntry           NVARCHAR(50)   NULL,
    dwh_create_date DATETIME2(7)   DEFAULT (SYSDATETIME()) NOT NULL,

    CountryID       AS cid             PERSISTED,
    CountryName     AS cntry           PERSISTED
);
GO


-- ===================================================================
-- Silver: ERP Customer Demographics (AZ12)
-- ===================================================================
IF OBJECT_ID(N'silver.erp_cust_az12') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;
GO

CREATE TABLE silver.erp_cust_az12
(
    cid             NVARCHAR(50)   NOT NULL,
    bdate           DATE           NULL,
    gen             NVARCHAR(50)   NULL,
    dwh_create_date DATETIME2(7)   DEFAULT (SYSDATETIME()) NOT NULL,

    CustomerID      AS cid             PERSISTED,
    BirthDate       AS bdate           PERSISTED,
    Gender          AS gen             PERSISTED
);
GO


-- ===================================================================
-- Silver: ERP Product Category Hierarchy (G1V2)
-- ===================================================================
IF EXISTS (SELECT * FROM sys.tables WHERE name = 'erp_px_cat_g1v2' AND schema_id = SCHEMA_ID('silver'))
    DROP TABLE silver.erp_px_cat_g1v2;
GO

CREATE TABLE silver.erp_px_cat_g1v2
(
    id              NVARCHAR(50)   NOT NULL,
    cat             NVARCHAR(50)   NULL,
    subcat          NVARCHAR(50)   NULL,
    maintenance     NVARCHAR(50)   NULL,
    dwh_create_date DATETIME2(7)   DEFAULT (SYSDATETIME()) NOT NULL,

    CategoryID      AS id              PERSISTED,
    Category        AS cat             PERSISTED,
    SubCategory     AS subcat          PERSISTED,
    MaintenanceFlag AS maintenance     PERSISTED
);
GO


-- ===================================================================
-- Final Confirmation
-- ===================================================================
PRINT N'';
PRINT N'All Silver layer tables have been successfully recreated.';
PRINT N'   → Audit column: dwh_create_date';
PRINT N'   → Clean aliases available for all columns (e.g., CustomerID, ProductName, SalesAmount)';
PRINT N'   → Ready for transformation into Gold layer.';
PRINT N'';
GO
