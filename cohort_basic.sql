-- Database Setup
CREATE DATABASE IF NOT EXISTS COHORT_ANALYSIS;
USE COHORT_ANALYSIS;

-- Data Exploration
SELECT * FROM sales_retail_ii_cleaned
LIMIT 1000;

-- How many rows (or records) are present in the table?
SELECT COUNT(*) as Total_records FROM sales_retail_ii_cleaned;

-- Unique InvoiceNo
select distinct InvoiceNo from sales_retail_ii_cleaned;

-- Unique StockCode 
select  StockCode from sales_retail_ii_cleaned;

-- Unique Description 
select  Description from sales_retail_ii_cleaned;

-- Unique Quantity 
select  distinct Quantity from sales_retail_ii_cleaned;

-- Unique InvoiceDate
select  distinct InvoiceDate from sales_retail_ii_cleaned;

-- Unique UnitPrice
select  distinct UnitPrice from sales_retail_ii_cleaned;

-- Unique CustomerID
select  distinct CustomerID from sales_retail_ii_cleaned;

-- Unique Country
select  distinct country from sales_retail_ii_cleaned;

-- InvoiceDate
SELECT INVOICEDATE FROM sales_retail_ii_cleaned
LIMIT 1000;

-- Max InvoiceDate
SELECT MAX(INVOICEDATE) FROM sales_retail_ii_cleaned; -- MM/DD/YYY HH:MM
SELECT INVOICEDATE FROM sales_retail_ii_cleaned;

WITH CustomerSpending AS (
    -- Step 1: Calculate total spending for each customer
    SELECT 
        CUSTOMERID,
        ROUND(SUM(QUANTITY * UNITPRICE),0) AS TOTAL_SPENDING
    FROM SALES_RETAIL_II_CLEANED
    GROUP BY CUSTOMERID
)
SELECT * FROM CUSTOMERSPENDING;









