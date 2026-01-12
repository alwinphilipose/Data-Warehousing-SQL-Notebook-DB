# Data Warehousing Project in Databricks (Delta + SQL)

## 📌 Project Overview
This project demonstrates how to design and build a simple **Data Warehouse** in Databricks using SQL.

It covers the complete lifecycle:
- Creation of source tables
- Staging layer
- Transformation layer
- Dimension tables
- Fact table
- Surrogate keys
- Incremental loading using SCD Type-1 with MERGE

The dataset used represents **Sales Orders** containing customers, products, geography and date information.

---

## 🧭 Architecture / Layers Used

The data warehouse follows a **layered architecture**:

### 1️⃣ Source Layer
- Database: `sales_scd`
- Table: `Orders`
- Contains original transactional sales data

### 2️⃣ Staging Layer
- Database: `orderDWH`
- Table: `stg_sales`
- Direct copy of source table
- Used to isolate raw data from transformations

### 3️⃣ Transformation Layer
- View: `trans_sales`
- Cleans data (example: removes NULL quantities)

### 4️⃣ Dimension Tables Created
| Dimension | Key | Description |
|----------|------|--------------|
| DimCustomers | DimCustomersKey | Customer attributes |
| DimProducts | DimProductsKey | Product attributes |
| DimRegion | DimRegionKey | Region & Country |
| DimDate | DimDateKey | Order Date |

Surrogate Keys generated using:
- `row_number()` window function

### 5️⃣ Fact Table
Fact table created:

- `FactSales`

Contains:
- Order metrics (quantity, price, amount)
- Foreign keys to dimension tables

Joins performed between fact and dimensions.

---

## 🧩 SCD Type-1 Implementation

### 🟢 Objective
Update dimension records when attributes change  
(no history kept — overwrite approach).

### ✔️ Steps Implemented
1. Create `DimProducts` dimension
2. Create a view with *new or changed product values*
3. Insert initial dimension data
4. Add new changed records into source table
5. Perform **MERGE**

### 🔁 Merge Logic
MERGE INTO sales_scd.DimProducts AS trg
USING sales_scd.view_DimProducts AS src
ON trg.ProductID = src.ProductID
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *


### 📝 Result
- Existing product attributes updated
- New products inserted
- No duplicate records
- No history tracking (Type-1 behavior)

---

## 🛠️ Technologies Used
- Databricks
- Delta Tables
- SQL
- Views
- MERGE command
- Window functions

---

## 🎯 What This Project Shows
- Understanding of data warehousing concepts
- Dimensional modeling
- Surrogate and business keys
- Fact-Dimension relationship
- SCD Type-1 slowly changing dimensions
- Databricks SQL development
- Incremental loading patterns

---

## 🚀 Possible Extensions (Future Work)
- Power BI dashboard on Fact table
- Add SCD Type-2 (history preservation)
- Implement CDC ingestion using Streaming
- Add Data Quality checks (Great Expectations / Delta Live Tables)
- Implement Medallion Architecture (Bronze-Silver-Gold)

---

## 👤 Author
Alwin Philipose  
(Feel free to connect on LinkedIn)
