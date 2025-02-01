# Azure End To End Data Pipeline SCD 1

## Overview

⁠•This project follows **medalion arcitechture**
•Car sales data is used, which is hosted on a **https** server.
•The project uses **SCD Type 1**, and processes data incrementally.
•Bronze Layer is maintained using **Azure Data Facory** and silver layer, gold layers are maintained    using Azure Databricks.
•ADLS Gen 2 is used for storage.
•Star Schema is maintained at the gold layer.
•**Unity Catalog** is used for data governance and data centralization.
•**Email Alerting** on success and failure

## Pipeline Flow

![Architechture Diagram.png](Azure%20End%20To%20End%20Data%20Pipeline%20SCD%201%2017932cf3d883802d99ccd6e06e6e19a6/Architechture_Diagram.png)

## Steps Followed

### Bronze Layer

- Pulled the raw data from https source and save it the **Azure SQL Database** using **Azure Datafactory.**
- Now the raw data is ingested incrementaly to **ADLS Gen 2** storage in parquet format using Azure Data Factory.
- Stored procedures are used to ensure incremental load.

### Silver Layer

- Azure Databricks is used further to process the silver and gold layer, the raw data which is stored in Bronze layer is then cleaned by applying various transformation according to the business rules.
- This data is then saved in the silver container in the Data Lake in delta format.

### Gold Layer

- Various dimension tables are derived from the silver layer, all these dimension tables load the data incrementally using delta merge feauture.
- Surrogate Keys are given for all the dimensions, which acts as the primary keys for the dimension tables.
- Finally  fact table is created containing all the neessary metrics and their identifier keys which can be used for seemless reporting.

## Azure Data Factory and Databricks Workflow

![ADF Pipeline.png](Azure%20End%20To%20End%20Data%20Pipeline%20SCD%201%2017932cf3d883802d99ccd6e06e6e19a6/98607ccd-1c11-4428-886a-994c34cde85d.png)