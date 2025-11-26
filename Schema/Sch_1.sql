/***********************************************************************
  Database Initialization Script – Data Warehouse Foundation
  Purpose : Safely (re)create the DataWarehouse database and its core schemas
  Schemas Created:
      • bronze  – Raw ingestion layer
      • silver  – Cleansed & conformed layer
      • gold    – Business-ready dimensional model
  WARNING: This script will DROP the existing DataWarehouse database!
***********************************************************************/

SET NOCOUNT ON;
SET QUOTED_IDENTIFIER ON;
GO

-- ================================================
-- Step 1: Ensure we are in the master database
-- ================================================
USE master;
GO

-- ================================================
-- Step 2: Drop existing DataWarehouse (if present)
-- ================================================
PRINT N'Checking for existing DataWarehouse database...';

IF DB_ID(N'DataWarehouse') IS NOT NULL
BEGIN
    PRINT N'→ Existing database found. Setting to single-user mode and dropping...';

    ALTER DATABASE DataWarehouse 
        SET SINGLE_USER 
        WITH ROLLBACK IMMEDIATE;

    DROP DATABASE DataWarehouse;

    PRINT N'→ DataWarehouse database dropped successfully.';
END
ELSE
BEGIN
    PRINT N'→ No existing DataWarehouse database found. Proceeding with creation.';
END
GO

-- ================================================
-- Step 3: Create fresh DataWarehouse database
-- ================================================
PRINT N'Creating new DataWarehouse database...';

CREATE DATABASE DataWarehouse
    CONTAINMENT = NONE
    ON PRIMARY 
    ( NAME = N'DataWarehouse_Data', 
      FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL16.MSSQLSERVER\MSSQL\DATA\DataWarehouse.mdf' ,
      SIZE = 512MB , 
      MAXSIZE = UNLIMITED, 
      FILEGROWTH = 64MB )
    LOG ON 
    ( NAME = N'DataWarehouse_Log', 
      FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL16.MSSQLSERVER\MSSQL\DATA\DataWarehouse_log.ldf' ,
      SIZE = 128MB , 
      MAXSIZE = 2TB , 
      FILEGROWTH = 64MB );

PRINT N'DataWarehouse database created successfully.';
GO

-- ================================================
-- Step 4: Switch context and create schemas
-- ================================================
USE DataWarehouse;
GO

PRINT N'Creating medallion architecture schemas...';

-- Bronze: Raw / Landing Zone
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'bronze')
BEGIN
    EXEC(N'CREATE SCHEMA bronze AUTHORIZATION dbo');
    PRINT N'   → Schema [bronze] created';
END

-- Silver: Refined & Integrated
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'silver')
BEGIN
    EXEC(N'CREATE SCHEMA silver AUTHORIZATION dbo');
    PRINT N'   → Schema [silver] created';
END

-- Gold: Business-Optimized (Star Schema / Reporting)
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'gold')
BEGIN
    EXEC(N'CREATE SCHEMA gold AUTHORIZATION dbo');
    PRINT N'   → Schema [gold] created';
END

-- ================================================
-- Final Confirmation
-- ================================================
PRINT N'';
PRINT N'════════════════════════════════════════════════════════════';
PRINT N'  DATA WAREHOUSE INITIALIZATION COMPLETED SUCCESSFULLY     ';
PRINT N'  Database : DataWarehouse                                  ';
PRINT N'  Schemas   : bronze, silver, gold                          ';
PRINT N'  Ready for bronze → silver → gold layer deployment        ';
PRINT N'════════════════════════════════════════════════════════════';
GO
