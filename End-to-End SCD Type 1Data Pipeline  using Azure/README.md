# Azure End-to-End Data Pipeline with SCD Type 1

## Overview  

- This project follows the **medallion architecture**.  
- Car sales data is used, which is hosted on an **HTTPS** server.  
- The project uses **SCD Type 1** and processes data incrementally.  
- The **Bronze Layer** is maintained using **Azure Data Factory**, while the **Silver and Gold Layers** are managed using **Azure Databricks**.  
- **ADLS Gen 2** is used for storage.  
- A **Star Schema** is maintained at the Gold Layer.  
- **Unity Catalog** is used for data governance and centralization.  
- **Email alerts** are triggered on success and failure.  

## Pipeline Flow  

![Architecture Diagram](https://github.com/user-attachments/assets/8581c1e8-98fb-4273-807f-aa7e2f7e8dd5)  

## Steps Followed  

### Bronze Layer  

- Pulled raw data from an **HTTPS** source and stored it in **Azure SQL Database** using **Azure Data Factory**.  
- Incrementally ingested raw data into **ADLS Gen 2** storage in **Parquet format** using **Azure Data Factory**.  
- Used **stored procedures** to ensure incremental loading.  

### Silver Layer  

- Processed the **Silver and Gold Layers** using **Azure Databricks**.  
- Cleaned raw data from the Bronze Layer by applying various **business rule transformations**.  
- Stored the transformed data in the **Silver container** in **Delta format**.  

### Gold Layer  

- Derived **dimension tables** from the Silver Layer, loading them incrementally using **Delta Merge**.  
- Assigned **Surrogate Keys** to all dimensions, serving as primary keys.  
- Created a **fact table** containing all key metrics and identifiers for seamless reporting.  

## Azure Data Factory and Databricks Workflow 

<img width="889" alt="ADF Pipeline" src="https://github.com/user-attachments/assets/45e0a5e8-9a52-4d22-86a1-a34f0a5099a2" />
<img width="1235" alt="Screenshot 2025-02-01 at 3 02 35 PM" src="https://github.com/user-attachments/assets/f7b6a4f2-c18a-4406-8cf9-bd430d9f84e2" />
<img width="1018" alt="Screenshot 2025-02-01 at 4 45 42 PM" src="https://github.com/user-attachments/assets/62b8c31e-4ac5-4c63-8a72-f6ebd0777710" />




