-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Incremental Data Loading

-- COMMAND ----------

CREATE DATABASE sales_scd;

-- COMMAND ----------

CREATE OR REPLACE TABLE sales_scd.Orders (
    OrderID INT,
    OrderDate DATE,
    CustomerID INT,
    CustomerName VARCHAR(100),
    CustomerEmail VARCHAR(100),
    ProductID INT,
    ProductName VARCHAR(100),
    ProductCategory VARCHAR(50),
    RegionID INT,
    RegionName VARCHAR(50),
    Country VARCHAR(50),
    Quantity INT,
    UnitPrice DECIMAL(10,2),
    TotalAmount DECIMAL(10,2)
);


-- COMMAND ----------

INSERT INTO sales_scd.Orders (OrderID, OrderDate, CustomerID, CustomerName, CustomerEmail, ProductID, ProductName, ProductCategory, RegionID, RegionName, Country, Quantity, UnitPrice, TotalAmount) 
VALUES 
(1, '2024-02-01', 101, 'Alice Johnson', 'alice@example.com', 201, 'Laptop', 'Electronics', 301, 'North America', 'USA', 2, 800.00, 1600.00),
(2, '2024-02-02', 102, 'Bob Smith', 'bob@example.com', 202, 'Smartphone', 'Electronics', 302, 'Europe', 'Germany', 1, 500.00, 500.00),
(3, '2024-02-03', 103, 'Charlie Brown', 'charlie@example.com', 203, 'Tablet', 'Electronics', 303, 'Asia', 'India', 3, 300.00, 900.00),
(4, '2024-02-04', 101, 'Alice Johnson', 'alice@example.com', 204, 'Headphones', 'Accessories', 301, 'North America', 'USA', 1, 150.00, 150.00),
(5, '2024-02-05', 104, 'David Lee', 'david@example.com', 205, 'Gaming Console', 'Electronics', 302, 'Europe', 'France', 1, 400.00, 400.00),
(6, '2024-02-06', 102, 'Bob Smith', 'bob@example.com', 206, 'Smartwatch', 'Electronics', 303, 'Asia', 'China', 2, 200.00, 400.00),
(7, '2024-02-07', 105, 'Eve Adams', 'eve@example.com', 201, 'Laptop', 'Electronics', 301, 'North America', 'Canada', 1, 800.00, 800.00),
(8, '2024-02-08', 106, 'Frank Miller', 'frank@example.com', 207, 'Monitor', 'Accessories', 302, 'Europe', 'Italy', 2, 250.00, 500.00),
(9, '2024-02-09', 107, 'Grace White', 'grace@example.com', 208, 'Keyboard', 'Accessories', 303, 'Asia', 'Japan', 3, 100.00, 300.00),
(10, '2024-02-10', 104, 'David Lee', 'david@example.com', 209, 'Mouse', 'Accessories', 301, 'North America', 'USA', 1, 50.00, 50.00);


-- COMMAND ----------

SELECT * FROM sales_new.orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # DATA WAREHOUSING

-- COMMAND ----------

CREATE DATABASE orderDWH

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Staging Layer

-- COMMAND ----------

CREATE OR REPLACE TABLE orderDWH.stg_sales 
AS 
SELECT * FROM sales_new.orders 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Transformation

-- COMMAND ----------

CREATE VIEW orderDWH.trans_sales
AS
SELECT * FROM orderDWH.stg_sales WHERE Quantity IS NOT NULL 

-- COMMAND ----------

SELECT * FROM orderdwh.trans_sales

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Core Layer 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### DimCustomers

-- COMMAND ----------

CREATE OR REPLACE TABLE orderDWH.DimCustomers 
(
  CustomerID INT,
  CustomerName STRING,
  CustomerEmail STRING,
  DimCustomersKey INT
)

-- COMMAND ----------

CREATE OR REPLACE VIEW orderDWH.view_DimCustomers
AS 
SELECT T.*,row_number() over(ORDER BY T.CustomerID) as DimCustomersKey FROM 
(
SELECT 
  DISTINCT(CustomerID) as CustomerID,
  CustomerName,
  CustomerEmail
FROM 
  orderDWH.trans_sales
) AS T

-- COMMAND ----------

SELECT * FROM orderDWH.view_DimCustomers

-- COMMAND ----------

INSERT INTO orderdwh.DimCustomers 
SELECT * FROM orderdwh.view_DimCustomers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### DimProducts

-- COMMAND ----------

CREATE TABLE orderDWH.DimProducts
(
  ProductID INT,
  ProductName STRING,
  ProductCategory STRING,
  DimProductsKey INT 
)

-- COMMAND ----------

CREATE OR REPLACE VIEW orderDWH.view_DimProducts
AS 
SELECT T.*,row_number() over(ORDER BY T.ProductID) as DimCustomersKey FROM 
(
SELECT 
  DISTINCT(ProductID) as ProductID,
  ProductName,
  ProductCategory
FROM 
  orderDWH.trans_sales
) AS T

-- COMMAND ----------

INSERT INTO orderdwh.DimProducts 
SELECT * FROM orderdwh.view_DimProducts

-- COMMAND ----------

SELECT * FROM orderdwh.DimProducts

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### DimRegion

-- COMMAND ----------

CREATE OR REPLACE TABLE orderDWH.DimRegion 
(
  RegionID INT,
  RegionName STRING,
  Country STRING,
  DimRegionKey INT
)

-- COMMAND ----------

CREATE OR REPLACE VIEW orderDWH.view_DimRegion
AS 
SELECT T.*,row_number() over(ORDER BY T.RegionID) as DimRegionKey FROM 
(
SELECT 
  DISTINCT(RegionID) as RegionID,
  RegionName,
  Country
FROM 
  orderDWH.trans_sales
) AS T

-- COMMAND ----------

INSERT INTO orderdwh.DimRegion
SELECT * FROM orderdwh.view_DimRegion

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### DimDate

-- COMMAND ----------

CREATE OR REPLACE TABLE orderDWH.DimDate
(
  OrderDate Date,
  DimDateKey INT
)

-- COMMAND ----------

CREATE OR REPLACE VIEW orderDWH.view_DimDate
AS 
SELECT T.*,row_number() over(ORDER BY T.OrderDate) as DimDateKey FROM 
(
SELECT 
  DISTINCT(OrderDate) as OrderDate
FROM 
  orderDWH.trans_sales
) AS T

-- COMMAND ----------

INSERT INTO orderdwh.DimDate
SELECT * FROM orderdwh.view_DimDate

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### FACT TABLE

-- COMMAND ----------

CREATE TABLE orderDWH.FactSales
(
  OrderID INT,
  Quantity DECIMAL,
  UnitPrice DECIMAL,
  TotalAmount DECIMAL,
  DimProductsKey INT,
  DimCustomersKeyu INT,
  DimRegionKey INT,
  DimDateKey INT
)

-- COMMAND ----------

SELECT 
  F.OrderID,
  F.Quantity,
  F.UnitPrice,
  F.TotalAmount,
  DC.DimCustomersKey,
  DP.DimProductsKey,
  DR.DimRegionKey,
  DD.DimDateKey
FROM  
  orderDWH.trans_sales F 
LEFT JOIN 
  orderDWH.DimCustomers DC 
  ON F.CustomerID = DC.CustomerID
LEFT JOIN 
  orderDWH.dimproducts DP 
  ON F.ProductID = DP.ProductID
LEFT JOIN 
  orderDWH.DimRegion DR 
  ON DR.Country = F.Country
LEFT JOIN 
  orderDWH.DimDate DD 
  ON F.OrderDate = DD.OrderDate







-- COMMAND ----------

SELECT * FROM orderdwh.DimRegion

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## SCD TYPE - 1

-- COMMAND ----------

SELECT * FROM sales_scd.orders

-- COMMAND ----------

CREATE OR REPLACE VIEW sales_scd.view_DimProducts
AS
SELECT DISTINCT(ProductID) as ProductID, ProductName, ProductCategory
FROM sales_scd.orders
WHERE OrderDate > '2024-02-10'

-- COMMAND ----------

CREATE OR REPLACE TABLE sales_scd.DimProducts 
(
  ProductID INT,
  ProductName STRING,
  ProductCategory STRING 
)

-- COMMAND ----------

INSERT INTO sales_scd.DimProducts
SELECT ProductID, ProductName, ProductCategory FROM sales_scd.view_DimProducts

-- COMMAND ----------

SELECT * FROM sales_scd.DimProducts

-- COMMAND ----------

INSERT INTO sales_scd.Orders (OrderID, OrderDate, CustomerID, CustomerName, CustomerEmail, ProductID, ProductName, ProductCategory, RegionID, RegionName, Country, Quantity, UnitPrice, TotalAmount) 
VALUES 
(1, '2024-02-11', 101, 'Alice Johnson', 'alice@example.com', 201, 'Gaming Laptop', 'Electronics', 301, 'North America', 'USA', 2, 800.00, 1600.00),
(2, '2024-02-12', 102, 'Bob Smith', 'bob@example.com', 230, 'Airpods', 'Electronics', 302, 'Europe', 'Germany', 1, 500.00, 500.00)

-- COMMAND ----------

SELECT * FROM sales_scd.view_DimProducts

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## MERGE - SCD TYPE - 1

-- COMMAND ----------

MERGE INTO sales_scd.DimProducts AS trg 
USING sales_scd.view_DimPrOducts AS src 
ON trg.ProductID = src.ProductID 
WHEN MATCHED THEN UPDATE SET * 
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

SELECT * FROM sales_scd.DimProducts

-- COMMAND ----------


