/***********************************************************************
  Stored Procedure: bronze.usp_LoadBronzeFromSource
  Purpose: Full refresh of all bronze-layer tables from raw CSV files
  Behavior: Truncate + Bulk Insert with meaningful column aliases
  Author : Data Engineering Team
***********************************************************************/

CREATE OR ALTER PROCEDURE bronze.usp_LoadBronzeFromSource
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE 
        @ProcStart      DATETIME2(3) = SYSDATETIME(),
        @StepStart      DATETIME2(3),
        @StepEnd        DATETIME2(3),
        @TotalSeconds   INT;

    BEGIN TRY
        PRINT N'';
        PRINT N'╔══════════════════════════════════════════════════════════════╗';
        PRINT N'║          STARTING BRONZE LAYER FULL LOAD                     ║';
        PRINT N'╚══════════════════════════════════════════════════════════════╝';
        PRINT N'';

        -- ==============================================================
        -- CRM DATA SOURCES
        -- ==============================================================

        PRINT N'Processing CRM Tables...';

        -- Customer Info
        SET @StepStart = SYSDATETIME();
        PRINT N'   → Truncating bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT N'   → Loading crm_cust_info with aliases...';
        INSERT INTO bronze.crm_cust_info WITH (TABLOCK)
        SELECT
            cst_id              AS CustomerID,
            cst_key             AS CustomerKey,
            cst_firstname       AS FirstName,
            cst_lastname        AS LastName,
            cst_marital_status  AS MaritalStatus,
            cst_gndr            AS Gender,
            cst_create_date     AS CustomerCreateDate
        FROM OPENROWSET(
            BULK 'C:\sql\dwh_project\datasets\source_crm\cust_info.csv',
            FORMAT = 'CSV',
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n'
        ) AS src;
        SET @StepEnd = SYSDATETIME();
        PRINT N'   → Completed in ' + FORMAT(DATEDIFF(MILLISECOND, @StepStart, @StepEnd), 'N0') + ' ms';

        -- Product Info
        SET @StepStart = SYSDATETIME();
        PRINT N'   → Truncating bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        INSERT INTO bronze.crm_prd_info WITH (TABLOCK)
        SELECT
            prd_id       AS ProductID,
            prd_key      AS ProductKey,
            prd_nm       AS ProductName,
            prd_cost     AS ProductCost,
            prd_line     AS ProductLine,
            prd_start_dt AS ProductStartDate,
            prd_end_dt   AS ProductEndDate
        FROM OPENROWSET(
            BULK 'C:\sql\dwh_project\datasets\source_crm\prd_info.csv',
            FORMAT = 'CSV',
            FIRSTROW = 2
        ) AS src;
        SET @StepEnd = SYSDATETIME();
        PRINT N'   → Completed in ' + FORMAT(DATEDIFF(MILLISECOND, @StepStart, @StepEnd), 'N0') + ' ms';

        -- Sales Details
        SET @StepStart = SYSDATETIME();
        PRINT N'   → Truncating bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        INSERT INTO bronze.crm_sales_details WITH (TABLOCK)
        SELECT
            sls_ord_num  AS SalesOrderNumber,
            sls_prd_key  AS ProductKey,
            sls_cust_id  AS CustomerID,
            sls_order_dt AS OrderDateKey,
            sls_ship_dt  AS ShipDateKey,
            sls_due_dt   AS DueDateKey,
            sls_sales    AS SalesAmount,
            sls_quantity AS Quantity,
            sls_price    AS UnitPrice
        FROM OPENROWSET(
            BULK 'C:\sql\dwh_project\datasets\source_crm\sales_details.csv',
            FORMAT = 'CSV',
            FIRSTROW = 2
        ) AS src;
        SET @StepEnd = SYSDATETIME();
        PRINT N'   → Completed in ' + FORMAT(DATEDIFF(MILLISECOND, @StepStart, @StepEnd), 'N0') + ' ms';

        PRINT N'CRM load section completed.';
        PRINT N'';

        -- ==============================================================
        -- ERP DATA SOURCES
        -- ==============================================================

        PRINT N'Processing ERP Tables...';

        -- Location A101
        SET @StepStart = SYSDATETIME();
        TRUNCATE TABLE bronze.erp_loc_a101;
        INSERT INTO bronze.erp_loc_a101 WITH (TABLOCK)
        SELECT cid AS CountryID, cntry AS CountryName
        FROM OPENROWSET(BULK 'C:\sql\dwh_project\datasets\source_erp\loc_a101.csv', FORMAT='CSV', FIRSTROW=2) AS s;
        PRINT N'   → erp_loc_a101 loaded';

        -- Customer AZ12
        SET @StepStart = SYSDATETIME();
        TRUNCATE TABLE bronze.erp_cust_az12;
        INSERT INTO bronze.erp_cust_az12 WITH (TABLOCK)
        SELECT cid AS CustomerID, bdate AS BirthDate, gen AS Gender
        FROM OPENROWSET(BULK 'C:\sql\dwh_project\datasets\source_erp\cust_az12.csv', FORMAT='CSV', FIRSTROW=2) AS s;
        PRINT N'   → erp_cust_az12 loaded';

        -- Product Category G1V2
        SET @StepStart = SYSDATETIME();
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        INSERT INTO bronze.erp_px_cat_g1v2 WITH (TABLOCK)
        SELECT 
            id           AS ProductID,
            cat          AS Category,
            subcat       AS SubCategory,
            maintenance  AS MaintenanceFlag
        FROM OPENROWSET(BULK 'C:\sql\dwh_project\datasets\source_erp\px_cat_g1v2.csv', FORMAT='CSV', FIRSTROW=2) AS s;
        PRINT N'   → erp_px_cat_g1v2 loaded';

        -- Final Summary
        SET @TotalSeconds = DATEDIFF(SECOND, @ProcStart, SYSDATETIME());

        PRINT N'';
        PRINT N'╔══════════════════════════════════════════════════════════════╗';
        PRINT N'║          BRONZE LAYER LOAD COMPLETED SUCCESSFULLY            ║';
        PRINT N'║ Total Duration: ' + FORMAT(@TotalSeconds, 'N0') + ' seconds (' + 
              FORMAT(@TotalSeconds / 60.0, 'N2') + ' minutes)                  ║';
        PRINT N'╚══════════════════════════════════════════════════════════════╝';

    END TRY
    BEGIN CATCH
        DECLARE @ErrMsg NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrNum INT = ERROR_NUMBER();
        DECLARE @ErrState INT = ERROR_STATE();

        PRINT N'';
        PRINT N'ERROR: Bronze load failed!';
        PRINT N'Error Number : ' + CAST(@ErrNum AS NVARCHAR(10));
        PRINT N'Error State  : ' + CAST(@ErrState AS NVARCHAR(10));
        PRINT N'Error Message: ' + @ErrMsg;
        PRINT N'';
        THROW;
    END CATCH
END;
GO

-- Execution example
-- EXEC bronze.usp_LoadBronzeFromSource;
