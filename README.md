# Data-Warehouse
## Creating a Data Warehouse using Medallion Architecture for better Analytics and Insight.
(Reproduced work of Mr. Baraa Khatib Salkini. Thanking him to be a great content creator)


### Summary of this Project

This repository demonstrates a **production-grade, modern data warehouse** built from scratch using the **Medallion Architecture** (Bronze → Silver → Gold), fully implemented in **SQL Server**.

From raw CSV files to actionable business insights — this project covers the **complete data lifecycle**:

| Layer        | Purpose                                      |
|-------------|------------------------------------------------------|
| **Bronze**  | Raw ingestion – land data as-is from source systems |
| **Silver**  | Cleansed, standardized, and integrated data         |
| **Gold**    | Business-ready dimensional model      |

---

### Key Features & Deliverables

## Key Features of the Project

# FeatureDescription1Medallion Architecture (Bronze → Silver → Gold)Full implementation of the modern data lakehouse/warehouse pattern for scalability and governance2Bronze Layer – Raw Ingestion• Lands data exactly as received from source systems (CRM & ERP CSVs)

• No transformations applied – preserves original format for auditability
• Uses high-performance OPENROWSET(BULK) / BULK INSERT with meaningful column aliases from day one3Silver Layer – Cleansed & Standardized• Comprehensive data quality fixes (trimming, deduplication, normalization)
• Gender & marital status standardized to readable values
• Product SCD Type 2 logic (ValidFrom/ValidTo)
• Category extraction from product keys
• Recalculated sales amounts & unit prices
• Persisted computed columns providing clean aliases (e.g., CustomerID, ProductName, SalesAmount) while keeping original column names4Gold Layer – Business-Ready Dimensional Model• Full Kimball star schema with surrogate keys
• dim_customers, dim_products, and fact_sales
• Enriched customer dimension (CRM + ERP merge with country & birthdate)
• Current-only product view (active versions)
• Ready for BI tools (Power BI, Tableau, SSRS)

