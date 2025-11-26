-- =====================================================================
-- Recreate Bronze Layer Tables (drop if exists + create fresh)
-- This script ensures clean bronze schema objects for raw data ingestion
-- =====================================================================

SET NOCOUNT ON;
GO

-- Customer master data from CRM system
IF EXISTS (SELECT 1 FROM sys.tables WHERE object_id = OBJECT_ID('bronze.crm_cust_info'))
    DROP TABLE bronze.crm_cust_info;
GO

CREATE TABLE bronze.crm_cust_info (
    cst_id              INT             NULL,
    cst_key             NVARCHAR(50)    NULL,
    cst_firstname       NVARCHAR(50)    NULL,
    cst_lastname        NVARCHAR(50)    NULL,
    cst_marital_status  NVARCHAR(50)    NULL,
    cst_gndr            NVARCHAR(50)    NULL,
    cst_create_date     DATE            NULL
);
GO


-- Product master data from CRM
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID('bronze.crm_prd_info') AND type = 'U')
    DROP TABLE bronze.crm_prd_info;
GO

CREATE TABLE bronze.crm_prd_info (
    prd_id       INT           NOT NULL,
    prd_key      NVARCHAR(50)  NOT NULL,
    prd_nm       NVARCHAR(50)  NULL,
    prd_cost     INT           NULL,
    prd_line     NVARCHAR(50)  NULL,
    prd_start_dt DATETIME      NULL,
    prd_end_dt   DATETIME      NULL
);
GO


-- Sales transaction details from CRM
IF OBJECT_ID(N'bronze.crm_sales_details', N'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;
GO

CREATE TABLE bronze.crm_sales_details (
    sls_ord_num   NVARCHAR(50)   NULL,
    sls_prd_key   NVARCHAR(50)   NULL,
    sls_cust_id   INT            NULL,
    sls_order_dt  INT            NULL,
    sls_ship_dt   INT            NULL,
    sls_due_dt    INT            NULL,
    sls_sales     INT            NULL,
    sls_quantity  INT            NULL,
    sls_price     INT            NULL
);
GO


-- ERP Location reference table (A101)
DROP TABLE IF EXISTS bronze.erp_loc_a101;
GO

CREATE TABLE bronze.erp_loc_a101 (
    cid    NVARCHAR(50)  NULL,
    cntry  NVARCHAR(50)  NULL
);
GO


-- ERP Customer extension table (AZ12)
DROP TABLE IF EXISTS bronze.erp_cust_az12;
GO

CREATE TABLE bronze.erp_cust_az12 (
    cid    NVARCHAR(50)   NULL,
    bdate  DATE           NULL,
    gen    NVARCHAR(50)   NULL
);
GO


-- ERP Product category hierarchy (G1V2)
IF OBJECT_ID('bronze.erp_px_cat_g1v2') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_g1v2;
GO

CREATE TABLE bronze.erp_px_cat_g1v2 (
    id           NVARCHAR(50)   NULL,
    cat          NVARCHAR(50)   NULL,
    subcat       NVARCHAR(50)   NULL,
    maintenance  NVARCHAR(50)   NULL
);
GO

-- Completion message
PRINT 'All bronze layer tables have been successfully (re)created.';
GO
