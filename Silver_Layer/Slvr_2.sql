/***********************************************************************
  Stored Procedure: silver.usp_LoadSilverFromBronze
  Purpose: Full cleanse & transform from Bronze → Silver layer
  Features:
    • Data quality fixes
    • Deduplication (latest record)
    • SCD Type 2 logic for products
    • Human-readable value mapping
    • Column aliases for clarity
***********************************************************************/

CREATE OR ALTER PROCEDURE silver.usp_LoadSilverFromBronze
AS
BEGIN
    SET NOCOUNT, XACT_ABORT ON;

    DECLARE 
        @ProcStart      DATETIME2(3) = SYSDATETIME(),
        @StepStart      DATETIME2(3),
        @StepEnd        DATETIME2(3),
        @ElapsedSec     INT;

    BEGIN TRY
        PRINT N'';
        PRINT N'════════════════════════════════════════════════════════════════';
        PRINT N'      STARTING SILVER LAYER LOAD (Bronze → Silver)            ';
        PRINT N'════════════════════════════════════════════════════════════════';
        PRINT N'';

        /* ============================================================= */
        /* 1. CRM Customer Master – Latest Record + Cleansing           */
        /* ============================================================= */
        SET @StepStart = SYSDATETIME();
        PRINT N'→ Processing silver.crm_cust_info (deduplicated + cleaned)';

        TRUNCATE TABLE silver.crm_cust_info;

        INSERT INTO silver.crm_cust_info (
            cst_id, cst_key, cst_firstname, cst_lastname, 
            cst_marital_status, cst_gndr, cst_create_date
        )
        SELECT 
            CustomerID          = cst_id,
            CustomerKey         = cst_key,
            FirstName           = LTRIM(RTRIM(cst_firstname)),
            LastName            = LTRIM(RTRIM(cst_lastname)),
            MaritalStatus       = CASE UPPER(LTRIM(RTRIM(cst_marital_status)))
                                     WHEN 'S' THEN 'Single'
                                     WHEN 'M' THEN 'Married'
                                     ELSE 'n/a' END,
            Gender              = CASE UPPER(LTRIM(RTRIM(cst_gndr)))
                                     WHEN 'F' THEN 'Female'
                                     WHEN 'M' THEN 'Male'
                                     ELSE 'n/a' END,
            CustomerSince       = cst_create_date
        FROM (
            SELECT *,
                   rn = ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC, cst_key)
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) AS ranked
        WHERE rn = 1;

        SET @StepEnd = SYSDATETIME();
        PRINT N'   Completed in ' + FORMAT(DATEDIFF(MS, @StepStart, @StepEnd), 'N0') + ' ms';
        PRINT N'';


        /* ============================================================= */
        /* 2. CRM Product Master – SCD2 + Category Extraction           */
        /* ============================================================= */
        SET @StepStart = SYSDATETIME();
        PRINT N'→ Processing silver.crm_prd_info (SCD Type 2 logic)';

        TRUNCATE TABLE silver.crm_prd_info;

        INSERT INTO silver.crm_prd_info (
            prd_id, cat_id, prd_key, prd_nm, prd_cost, 
            prd_line, prd_start_dt, prd_end_dt
        )
        SELECT
            ProductID       = prd_id,
            CategoryID      = REPLACE(LEFT(prd_key, 5), '-', '_'),
            ProductKey      = SUBSTRING(prd_key, 7, 100),
            ProductName     = prd_nm,
            UnitCost        = COALESCE(prd_cost, 0),
            ProductLine     = CASE UPPER(LTRIM(RTRIM(prd_line)))
                                 WHEN 'M' THEN 'Mountain'
                                 WHEN 'R' THEN 'Road'
                                 WHEN 'T' THEN 'Touring'
                                 WHEN 'S' THEN 'Other Sales'
                                 ELSE 'n/a' END,
            ValidFrom       = TRY_CAST(prd_start_dt AS DATE),
            ValidTo         = DATEADD(DAY, -1, 
                                 LEAD(TRY_CAST(prd_start_dt AS DATE)) 
                                 OVER (PARTITION BY SUBSTRING(prd_key, 7, 100) ORDER BY prd_start_dt))
        FROM bronze.crm_prd_info;

        SET @StepEnd = SYSDATETIME();
        PRINT N'   Completed in ' + FORMAT(DATEDIFF(MS, @StepStart, @StepEnd), 'N0') + ' ms';
        PRINT N'';


        /* ============================================================= */
        /* 3. CRM Sales Transactions – Date & Amount Fix                 */
        /* ============================================================= */
        SET @StepStart = SYSDATETIME();
        PRINT N'→ Processing silver.crm_sales_details (data quality fixes)';

        TRUNCATE TABLE silver.crm_sales_details;

        INSERT INTO silver.crm_sales_details (
            sls_ord_num, sls_prd_key, sls_cust_id,
            sls_order_dt, sls_ship_dt, sls_due_dt,
            sls_sales, sls_quantity, sls_price
        )
        SELECT
            OrderNumber     = sls_ord_num,
            ProductKey      = sls_prd_key,
            CustomerID      = sls_cust_id,
            OrderDate       = TRY_CONVERT(DATE, CONVERT(VARCHAR(8), sls_order_dt)),
            ShipDate        = TRY_CONVERT(DATE, CONVERT(VARCHAR(8), sls_ship_dt)),
            DueDate         = TRY_CONVERT(DATE, CONVERT(VARCHAR(8), sls_due_dt)),
            SalesAmount     = COALESCE(NULLIF(sls_sales, 0), sls_quantity * ABS(sls_price)),
            Quantity        = sls_quantity,
            UnitPrice       = CASE 
                                WHEN sls_price > 0 THEN sls_price
                                ELSE ROUND(sls_sales * 1.0 / NULLIF(sls_quantity, 0), 2)
                              END
        FROM bronze.crm_sales_details;

        SET @StepEnd = SYSDATETIME();
        PRINT N'   Completed in ' + FORMAT(DATEDIFF(MS, @StepStart, @StepEnd), 'N0') + ' ms';
        PRINT N'';


        /* ============================================================= */
        /* 4. ERP Customer Demographics – Cleanse                        */
        /* ============================================================= */
        SET @StepStart = SYSDATETIME();
        PRINT N'→ Processing silver.erp_cust_az12';

        TRUNCATE TABLE silver.erp_cust_az12;

        INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
        SELECT
            CustomerID = CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, 50) ELSE cid END,
            BirthDate  = IIF(bdate > SYSDATETIME(), NULL, bdate),
            Gender     = CASE UPPER(LTRIM(RTRIM(gen)))
                            WHEN 'F' WHEN 'FEMALE' THEN 'Female'
                            WHEN 'M' WHEN 'MALE'   THEN 'Male'
                            ELSE 'n/a' END
        FROM bronze.erp_cust_az12;

        SET @StepEnd = SYSDATETIME();
        PRINT N'   Completed in ' + FORMAT(DATEDIFF(MS, @StepStart, @StepEnd), 'N0') + ' ms';


        /* ============================================================= */
        /* 5. ERP Location – Country Name Normalization                  */
        /* ============================================================= */
        SET @StepStart = SYSDATETIME();
        PRINT N'→ Processing silver.erp_loc_a101';

        TRUNCATE TABLE silver.erp_loc_a101;

        INSERT INTO silver.erp_loc_a101 (cid, cntry)
        SELECT
            CountryID   = REPLACE(cid, '-', ''),
            CountryName = CASE UPPER(TRIM(cntry))
                            WHEN 'DE'       THEN 'Germany'
                            WHEN 'US' WHEN 'USA' THEN 'United States'
                            WHEN '' WHEN NULL THEN 'n/a'
                            ELSE TRIM(cntry)
                          END
        FROM bronze.erp_loc_a101;

        SET @StepEnd = SYSDATETIME();
        PRINT N'   Completed in ' + FORMAT(DATEDIFF(MS, @StepStart, @StepEnd), 'N0') + ' ms';


        /* ============================================================= */
        /* 6. ERP Product Category – Direct Load                         */
        /* ============================================================= */
        SET @StepStart = SYSDATETIME();
        PRINT N'→ Processing silver.erp_px_cat_g1v2 (pass-through)';

        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
        SELECT 
            CategoryID      = id,
            Category        = cat,
            SubCategory     = subcat,
            MaintenanceFlag = maintenance
        FROM bronze.erp_px_cat_g1v2;

        SET @StepEnd = SYSDATETIME();
        PRINT N'   Completed in ' + FORMAT(DATEDIFF(MS, @StepStart, @StepEnd), 'N0') + ' ms';


        /* ============================================================= */
        /* Final Summary                                                 */
        /* ============================================================= */
        DECLARE @TotalSec INT = DATEDIFF(SECOND, @ProcStart, SYSDATETIME());

        PRINT N'';
        PRINT N'════════════════════════════════════════════════════════════════';
        PRINT N'      SILVER LAYER LOAD COMPLETED SUCCESSFULLY                 ';
        PRINT N'      Total Duration: ' + FORMAT(@TotalSec, 'N0') + ' seconds (' + 
              FORMAT(@TotalSec / 60.0, 'N2') + ' minutes)';
        PRINT N'════════════════════════════════════════════════════════════════';

    END TRY
    BEGIN CATCH
        DECLARE @ErrMsg NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrNo  INT           = ERROR_NUMBER();

        PRINT N'';
        PRINT N'CRITICAL ERROR – Silver load failed!';
        PRINT N'Error #' + FORMAT(@ErrNo, 'N0') + ': ' + @ErrMsg;
        PRINT N'Procedure: silver.usp_LoadSilverFromBronze';
        PRINT N'Timestamp : ' + CONVERT(VARCHAR(30), SYSDATETIME(), 120);

        THROW;
    END CATCH
END;
GO

-- Example execution
-- EXEC silver.usp_LoadSilverFromBronze;
