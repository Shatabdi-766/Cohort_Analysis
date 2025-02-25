-- find the customer revenue
SELECT 
    INVOICEDATE,
    CUSTOMERID,
    ROUND(QUANTITY * UNITPRICE, 2) AS REVENUE
FROM
    sales_retail_ii_cleaned
WHERE
    CustomerID IS NOT NULL
        AND CustomerID <> ''
ORDER BY CustomerID; 

-- First Purchase Month
SELECT InvoiceNo,
CustomerID,
InvoiceDate,
date_format(INVOICEDATE,'%Y-%m-01') AS PURCHASE_MONTH,
date_format(MIN(INVOICEDATE) OVER (PARTITION BY CustomerID ORDER BY INVOICEDATE) ,'%Y-%m-01') AS FIRST_PURCHASE_MONTH
FROM sales_retail_ii_cleaned;

-- Find the Customer Revenue CTE
WITH CTE1 AS (
SELECT 
    INVOICEDATE,
    CUSTOMERID,
    abs(ROUND(QUANTITY * UNITPRICE, 2)) AS REVENUE
FROM
    sales_retail_ii_cleaned
WHERE
    CustomerID IS NOT NULL
ORDER BY CustomerID
) 

SELECT * FROM CTE1;

-- Find the first purchase date for each customer
SELECT CUSTOMERID,
MIN(INVOICEDATE) AS FIRST_PURCHASE_DATE
FROM sales_retail_ii_cleaned
GROUP BY CustomerID;

-- Purchase Month CTE
WITH CTE1 AS (
SELECT 
    InvoiceNo,
    InvoiceDate,
    CUSTOMERID,
    abs(ROUND(QUANTITY * UNITPRICE, 2)) AS REVENUE
FROM
    sales_retail_ii_cleaned
WHERE
    CustomerID IS NOT NULL
ORDER BY CustomerID
),

CTE2 AS (
SELECT 
        InvoiceNo, 
        CUSTOMERID, 
        INVOICEDATE, 
        DATE_FORMAT(INVOICEDATE, '%Y-%m-01') AS PURCHASE_MONTH,
        DATE_FORMAT(MIN(INVOICEDATE) OVER (PARTITION BY CUSTOMERID ORDER BY INVOICEDATE), '%Y-%m-01') AS 
        FIRST_PURCHASE_MONTH,
        REVENUE
    FROM CTE1

)
SELECT * FROM CTE2;

-- The First and Second Purchase Dates
-- Step 1: Rank purchases for each customer
WITH CTE1 AS (
    SELECT 
        CUSTOMERID,
        INVOICEDATE,
        ROW_NUMBER() OVER (PARTITION BY CUSTOMERID ORDER BY INVOICEDATE) AS PURCHASE_RANK
    FROM sales_retail_ii_cleaned
),

-- Step 2: Extract the first and second purchase dates
CTE2 AS (
    SELECT 
        CUSTOMERID,
        MAX(CASE WHEN PURCHASE_RANK = 1 THEN INVOICEDATE END) AS FIRST_PURCHASE_DATE,
        MAX(CASE WHEN PURCHASE_RANK = 2 THEN INVOICEDATE END) AS SECOND_PURCHASE_DATE
    FROM CTE1
    WHERE PURCHASE_RANK IN (1, 2)
    GROUP BY CUSTOMERID
)

-- Step 3: Select the first and second purchase dates for each customer
SELECT 
    CUSTOMERID,
    FIRST_PURCHASE_DATE,
    SECOND_PURCHASE_DATE
FROM CTE2;

-- Identify customers who made a purchase in the second month
WITH FIRST_PURCHASE_DATE AS (

-- Find the first purchase date for each customer
SELECT CUSTOMERID,
MIN(INVOICEDATE) AS FIRST_PURCHASE_DATE
FROM sales_retail_ii_cleaned
GROUP BY CustomerID

),

SECOND_MONTH_ACTIVITY AS (
-- Calculate the start and end of the second month after the first purchase
SELECT FP.CUSTOMERID,
date_add(FP.FIRST_PURCHASE_DATE, INTERVAL 1 MONTH) AS START_SECOND_PURCHASE_DATE,
date_add(FP.FIRST_PURCHASE_DATE, INTERVAL 2 MONTH) AS END_SECOND_PURCHASE_DATE
FROM FIRST_PURCHASE_DATE FP

),

SECOND_PURCHASE_DATE AS (
    -- Step 3: Identify customers who made a purchase in the second month
    SELECT 
        SMA.CUSTOMERID,
        SMA.START_SECOND_PURCHASE_DATE AS Sec_M_PURCHASE_DATE
    FROM SECOND_MONTH_ACTIVITY SMA
    JOIN SALES_RETAIL_II_CLEANED SR
        ON SMA.CUSTOMERID = SR.CUSTOMERID
        AND SR.INVOICEDATE >= SMA.START_SECOND_PURCHASE_DATE
        AND SR.INVOICEDATE < SMA.END_SECOND_PURCHASE_DATE
        GROUP BY 1,2
    
)

SELECT * FROM SECOND_PURCHASE_DATE;

-- Identify customers who made a purchase in the third month
WITH FirstPurchase AS (
    -- Step 1: Find the first purchase date for each customer
    SELECT 
        CUSTOMERID,
        MIN(INVOICEDATE) AS FIRST_PURCHASE_DATE,
        QUARTER(MIN(INVOICEDATE)) AS JOIN_QUARTER
    FROM SALES_RETAIL_II_CLEANED
    GROUP BY CUSTOMERID
),

ThirdMonthActivity AS (
    -- Step 2: Calculate the start and end of the third month after the first purchase
    SELECT 
        FP.CUSTOMERID,
        FP.JOIN_QUARTER,
        DATE_ADD(FP.FIRST_PURCHASE_DATE, INTERVAL 2 MONTH) AS START_THIRD_MONTH,
        DATE_ADD(FP.FIRST_PURCHASE_DATE, INTERVAL 3 MONTH) AS END_THIRD_MONTH
    FROM FirstPurchase FP
),

ThirdMonthPurchases AS (
    -- Step 3: Identify customers who made a purchase in the third month
    SELECT 
        TMA.CUSTOMERID,
        TMA.JOIN_QUARTER
    FROM ThirdMonthActivity TMA
    JOIN SALES_RETAIL_II_CLEANED SR
        ON TMA.CUSTOMERID = SR.CUSTOMERID
        AND SR.INVOICEDATE >= TMA.START_THIRD_MONTH
        AND SR.INVOICEDATE < TMA.END_THIRD_MONTH
    GROUP BY TMA.CUSTOMERID, TMA.JOIN_QUARTER
)

SELECT * FROM THIRDMONTHPURCHASES;

-- Calculate the start and end of the second month after the first purchase

WITH FIRST_PURCHASE_DATE AS (

-- Find the first purchase date for each customer
SELECT CUSTOMERID,
MIN(INVOICEDATE) AS FIRST_PURCHASE_DATE
FROM sales_retail_ii_cleaned
GROUP BY CustomerID

),

SECOND_MONTH_ACTIVITY AS (
-- Calculate the start and end of the second month after the first purchase
SELECT FP.CUSTOMERID,
date_add(FP.FIRST_PURCHASE_DATE, INTERVAL 1 MONTH) AS START_SECOND_PURCHASE_DATE,
date_add(FP.FIRST_PURCHASE_DATE, INTERVAL 2 MONTH) AS END_SECOND_PURCHASE_DATE
FROM FIRST_PURCHASE_DATE FP

)

SELECT * FROM SECOND_MONTH_ACTIVITY;


-- analyze the frequency of repeat purchases for each cohort
WITH FirstPurchase AS (
    -- Step 1: Find the first purchase date for each customer
    SELECT 
        CUSTOMERID,
        MIN(INVOICEDATE) AS FIRST_PURCHASE_DATE,
        DATE_FORMAT(MIN(INVOICEDATE), '%Y-%m') AS COHORT_MONTH
    FROM SALES_RETAIL_II_CLEANED
    GROUP BY CUSTOMERID
),

RepeatPurchases AS (
    -- Step 2: Count the number of repeat purchases for each customer
    SELECT 
        FP.CUSTOMERID,
        FP.COHORT_MONTH,
        COUNT(SR.INVOICENO) - 1 AS REPEAT_PURCHASE_COUNT
    FROM FirstPurchase FP
    JOIN SALES_RETAIL_II_CLEANED SR
        ON FP.CUSTOMERID = SR.CUSTOMERID
        AND SR.INVOICEDATE > FP.FIRST_PURCHASE_DATE
    GROUP BY FP.CUSTOMERID, FP.COHORT_MONTH
)

-- Step 3: Calculate the average repeat purchase count for each cohort
SELECT 
    COHORT_MONTH,
    AVG(REPEAT_PURCHASE_COUNT) AS AVG_REPEAT_PURCHASES
FROM RepeatPurchases
GROUP BY COHORT_MONTH
ORDER BY COHORT_MONTH;

-- Calculate percentiles for spending group
WITH CustomerSpending AS (
    -- Step 1: Calculate total spending for each customer
    SELECT 
        CUSTOMERID,
        ROUND(SUM(QUANTITY * UNITPRICE),0) AS TOTAL_SPENDING
    FROM SALES_RETAIL_II_CLEANED
    GROUP BY CUSTOMERID
),

SPENDINGPERCENTILE AS (
    -- Step 2: Calculate percentiles for spending

SELECT 
CUSTOMERID,
TOTAL_SPENDING,
NTILE(5) OVER (order by TOTAL_SPENDING DESC) AS SPENDING_GROUP
FROM CustomerSpending
)

SELECT * FROM SPENDINGPERCENTILE;

-- Cross-Cohort Comparison:
-- Calculate the activity of each customer by month
WITH FirstPurchase AS (
    -- Step 1: Find the first purchase date for each customer
    SELECT 
        CUSTOMERID,
        MIN(INVOICEDATE) AS FIRST_PURCHASE_DATE,
        DATE_FORMAT(MIN(INVOICEDATE), '%Y-%m') AS COHORT_MONTH
    FROM SALES_RETAIL_II_CLEANED
    GROUP BY CUSTOMERID
),
MonthlyActivity AS (
    -- Step 2: Calculate the activity of each customer by month
    SELECT 
        CUSTOMERID,
        DATE_FORMAT(INVOICEDATE, '%Y-%m') AS ACTIVITY_MONTH
    FROM SALES_RETAIL_II_CLEANED
    GROUP BY CUSTOMERID, DATE_FORMAT(INVOICEDATE, '%Y-%m')
)
SELECT * FROM MONTHLYACTIVITY;

-- calculate the percentage of customers who made a purchase in the second month after their first purchase.
WITH FIRSTPURCHASE AS (
    -- Step 1: Find the first purchase date for each customer
    SELECT 
        CUSTOMERID,
        MIN(INVOICEDATE) AS FIRST_PURCHASE_DATE
    FROM SALES_RETAIL_II_CLEANED
    GROUP BY CUSTOMERID
),

SECOND_MONTH_ACTIVITY AS (
    -- Step 2: Calculate the start and end of the second month after the first purchase
    SELECT 
        FP.CUSTOMERID,
        DATE_ADD(FP.FIRST_PURCHASE_DATE, INTERVAL 1 MONTH) AS START_SECOND_PURCHASE_DATE,
        DATE_ADD(FP.FIRST_PURCHASE_DATE, INTERVAL 2 MONTH) AS END_SECOND_PURCHASE_DATE
    FROM FIRSTPURCHASE FP
),

SECOND_PURCHASE_DATE AS (
    -- Step 3: Identify customers who made a purchase in the second month
    SELECT 
        SMA.CUSTOMERID
    FROM SECOND_MONTH_ACTIVITY SMA
    JOIN SALES_RETAIL_II_CLEANED SR
        ON SMA.CUSTOMERID = SR.CUSTOMERID
        AND SR.INVOICEDATE >= SMA.START_SECOND_PURCHASE_DATE
        AND SR.INVOICEDATE < SMA.END_SECOND_PURCHASE_DATE
    GROUP BY SMA.CUSTOMERID
)

-- Step 4: Calculate the percentage of customers who made a purchase in the second month
SELECT 
    COUNT(DISTINCT SPD.CUSTOMERID) * 100.0 / COUNT(DISTINCT FP.CUSTOMERID) AS PercentageRetained
FROM FIRSTPURCHASE FP
LEFT JOIN SECOND_PURCHASE_DATE SPD
    ON FP.CUSTOMERID = SPD.CUSTOMERID;
    
    -- COHORT Month
WITH CTE1 AS (
SELECT 
    InvoiceNo,
    InvoiceDate,
    CUSTOMERID,
    abs(ROUND(QUANTITY * UNITPRICE, 2)) AS REVENUE
FROM
    sales_retail_ii_cleaned
WHERE
    CustomerID IS NOT NULL
ORDER BY CustomerID
),

CTE2 AS (
SELECT 
        InvoiceNo, 
        CUSTOMERID, 
        INVOICEDATE, 
        DATE_FORMAT(INVOICEDATE, '%Y-%m-01') AS PURCHASE_MONTH,
        DATE_FORMAT(MIN(INVOICEDATE) OVER (PARTITION BY CUSTOMERID ORDER BY INVOICEDATE), '%Y-%m-01') AS 
        FIRST_PURCHASE_MONTH,
        REVENUE
    FROM CTE1

),
CTE3 AS (
SELECT 
CUSTOMERID,
FIRST_PURCHASE_MONTH,
concat(
'MONTH_',
PERIOD_DIFF(
EXTRACT(YEAR_MONTH FROM PURCHASE_MONTH),
EXTRACT(YEAR_MONTH FROM FIRST_PURCHASE_MONTH)
)) AS COHORT_MONTH
FROM CTE2
)
SELECT * FROM CTE3;

-- compare the 3-month retention rates of customers who joined in Q1 vs. Q2.
WITH FirstPurchase AS (
    -- Step 1: Find the first purchase date for each customer
    SELECT 
        CUSTOMERID,
        MIN(INVOICEDATE) AS FIRST_PURCHASE_DATE,
        QUARTER(MIN(INVOICEDATE)) AS JOIN_QUARTER
    FROM SALES_RETAIL_II_CLEANED
    GROUP BY CUSTOMERID
),

ThirdMonthActivity AS (
    -- Step 2: Calculate the start and end of the third month after the first purchase
    SELECT 
        FP.CUSTOMERID,
        FP.JOIN_QUARTER,
        DATE_ADD(FP.FIRST_PURCHASE_DATE, INTERVAL 2 MONTH) AS START_THIRD_MONTH,
        DATE_ADD(FP.FIRST_PURCHASE_DATE, INTERVAL 3 MONTH) AS END_THIRD_MONTH
    FROM FirstPurchase FP
),

ThirdMonthPurchases AS (
    -- Step 3: Identify customers who made a purchase in the third month
    SELECT 
        TMA.CUSTOMERID,
        TMA.JOIN_QUARTER
    FROM ThirdMonthActivity TMA
    JOIN SALES_RETAIL_II_CLEANED SR
        ON TMA.CUSTOMERID = SR.CUSTOMERID
        AND SR.INVOICEDATE >= TMA.START_THIRD_MONTH
        AND SR.INVOICEDATE < TMA.END_THIRD_MONTH
    GROUP BY TMA.CUSTOMERID, TMA.JOIN_QUARTER
)

-- Step 4: Calculate the 3-month retention rate for Q1 and Q2 cohorts
SELECT 
    FP.JOIN_QUARTER,
    COUNT(DISTINCT TMP.CUSTOMERID) * 100.0 / COUNT(DISTINCT FP.CUSTOMERID) AS THREE_MONTH_RETENTION_RATE
FROM FirstPurchase FP
LEFT JOIN ThirdMonthPurchases TMP
    ON FP.CUSTOMERID = TMP.CUSTOMERID
WHERE FP.JOIN_QUARTER IN (1, 2) -- Filter for Q1 and Q2 cohorts
GROUP BY FP.JOIN_QUARTER
ORDER BY FP.JOIN_QUARTER;

-- Cross-Cohort Comparison:
-- How would you compare the retention rates of two different cohorts (e.g., customers who joined in January vs. February)?
WITH FirstPurchase AS (
    -- Step 1: Find the first purchase date for each customer
    SELECT 
        CUSTOMERID,
        MIN(INVOICEDATE) AS FIRST_PURCHASE_DATE,
        DATE_FORMAT(MIN(INVOICEDATE), '%Y-%m') AS COHORT_MONTH
    FROM SALES_RETAIL_II_CLEANED
    GROUP BY CUSTOMERID
),
MonthlyActivity AS (
    -- Step 2: Calculate the activity of each customer by month
    SELECT 
        CUSTOMERID,
        DATE_FORMAT(INVOICEDATE, '%Y-%m') AS ACTIVITY_MONTH
    FROM SALES_RETAIL_II_CLEANED
    GROUP BY CUSTOMERID, DATE_FORMAT(INVOICEDATE, '%Y-%m')
),

CohortRetention AS (
    -- Step 3: Calculate retention for each cohort
    SELECT 
    FP.COHORT_MONTH,
    MA.ACTIVITY_MONTH,
    COUNT(DISTINCT FP.CUSTOMERID) AS COHORT_SIZE,
    COUNT(DISTINCT MA.CUSTOMERID) AS ACTIVE_CUSTOMERS,
    COUNT(DISTINCT MA.CUSTOMERID )*100.0 / COUNT(DISTINCT FP.CUSTOMERID) AS RETENTION_RATE
	FROM FirstPurchase FP
    LEFT JOIN MonthlyActivity MA
        ON FP.CUSTOMERID = MA.CUSTOMERID
        AND MA.ACTIVITY_MONTH >= FP.COHORT_MONTH
    GROUP BY FP.COHORT_MONTH, MA.ACTIVITY_MONTH
)

-- Step 4: Compare retention rates for January and February cohorts
     SELECT 
    COHORT_MONTH, ACTIVITY_MONTH, RETENTION_RATE
FROM
    CohortRetention
WHERE
    COHORT_MONTH IN ('2011-01' , '2011-02')
ORDER BY COHORT_MONTH , ACTIVITY_MONTH;

-- Count the number of repeat purchases for each customer
WITH FirstPurchase AS (
    -- Step 1: Find the first purchase date for each customer
    SELECT 
        CUSTOMERID,
        MIN(INVOICEDATE) AS FIRST_PURCHASE_DATE,
        DATE_FORMAT(MIN(INVOICEDATE), '%Y-%m') AS COHORT_MONTH
    FROM SALES_RETAIL_II_CLEANED
    GROUP BY CUSTOMERID
),

RepeatPurchases AS (
    -- Step 2: Count the number of repeat purchases for each customer
    SELECT 
        FP.CUSTOMERID,
        FP.COHORT_MONTH,
        COUNT(SR.INVOICENO) - 1 AS REPEAT_PURCHASE_COUNT
    FROM FirstPurchase FP
    JOIN SALES_RETAIL_II_CLEANED SR
        ON FP.CUSTOMERID = SR.CUSTOMERID
        AND SR.INVOICEDATE > FP.FIRST_PURCHASE_DATE
    GROUP BY FP.CUSTOMERID, FP.COHORT_MONTH
)

SELECT * FROM RepeatPurchases;

-- create cohorts based on customer behavior, such as high-spending vs. low-spending customers
WITH CustomerSpending AS (
    -- Step 1: Calculate total spending for each customer
    SELECT 
        CUSTOMERID,
        ROUND(SUM(QUANTITY * UNITPRICE),0) AS TOTAL_SPENDING
    FROM SALES_RETAIL_II_CLEANED
    GROUP BY CUSTOMERID
)
-- Step 2: Assign customers to cohorts based on fixed thresholds
SELECT 
CUSTOMERID,
TOTAL_SPENDING,
CASE
WHEN TOTAL_SPENDING > 1000 THEN 'HIGH_SPENDING'
WHEN TOTAL_SPENDING between 500 AND 1000 THEN 'MEDIUM_SPENDING'
WHEN TOTAL_SPENDING < 500 THEN 'LOW_SPENDING'
END AS SPENDING_COHORT
FROM CustomerSpending
ORDER BY TOTAL_SPENDING DESC;


-- Distribution of Repeat Purchases
WITH FirstPurchase AS (
    -- Step 1: Find the first purchase date for each customer
    SELECT 
        CUSTOMERID,
        MIN(INVOICEDATE) AS FIRST_PURCHASE_DATE,
        DATE_FORMAT(MIN(INVOICEDATE), '%Y-%m') AS COHORT_MONTH
    FROM SALES_RETAIL_II_CLEANED
    GROUP BY CUSTOMERID
),

RepeatPurchases AS (
    -- Step 2: Count the number of repeat purchases for each customer
    SELECT 
        FP.CUSTOMERID,
        FP.COHORT_MONTH,
        COUNT(SR.INVOICENO) - 1 AS REPEAT_PURCHASE_COUNT
    FROM FirstPurchase FP
    JOIN SALES_RETAIL_II_CLEANED SR
        ON FP.CUSTOMERID = SR.CUSTOMERID
        AND SR.INVOICEDATE > FP.FIRST_PURCHASE_DATE
    GROUP BY FP.CUSTOMERID, FP.COHORT_MONTH
)

-- Step 3: Calculate the distribution of repeat purchases for each cohort
SELECT 
    COHORT_MONTH,
    REPEAT_PURCHASE_COUNT,
    COUNT(CUSTOMERID) AS CUSTOMER_COUNT
FROM RepeatPurchases
GROUP BY COHORT_MONTH, REPEAT_PURCHASE_COUNT
ORDER BY COHORT_MONTH, REPEAT_PURCHASE_COUNT ASC;

--  segment customers into cohorts based on their 
-- first purchased product category and calculate their 6-month retention rates.
WITH FirstPurchase AS (
    -- Step 1: Find the first purchase date and product category for each customer
    SELECT 
        CUSTOMERID,
        MIN(INVOICEDATE) AS FIRST_PURCHASE_DATE,
        SUBSTRING_INDEX(GROUP_CONCAT(StockCode ORDER BY INVOICEDATE), ',', 1) AS FIRST_PRODUCT_CATEGORY
    FROM SALES_RETAIL_II_CLEANED
    GROUP BY CUSTOMERID
),

SixMonthActivity AS (
    -- Step 2: Calculate the end of the 6-month period after the first purchase
    SELECT 
        FP.CUSTOMERID,
        FP.FIRST_PRODUCT_CATEGORY,
        FP.FIRST_PURCHASE_DATE,
        DATE_ADD(FP.FIRST_PURCHASE_DATE, INTERVAL 6 MONTH) AS SIX_MONTH_END_DATE
    FROM FirstPurchase FP
),

Retention AS (
    -- Step 3: Identify customers who made a purchase within 6 months after their first purchase
    SELECT 
        SMA.CUSTOMERID,
        SMA.FIRST_PRODUCT_CATEGORY
    FROM SixMonthActivity SMA
    JOIN SALES_RETAIL_II_CLEANED SR
        ON SMA.CUSTOMERID = SR.CUSTOMERID
        AND SR.INVOICEDATE > SMA.FIRST_PURCHASE_DATE
        AND SR.INVOICEDATE <= SMA.SIX_MONTH_END_DATE
    GROUP BY SMA.CUSTOMERID, SMA.FIRST_PRODUCT_CATEGORY
)

-- Step 4: Calculate the 6-month retention rate for each product category cohort
SELECT 
    FP.FIRST_PRODUCT_CATEGORY,
    COUNT(DISTINCT R.CUSTOMERID) * 100.0 / COUNT(DISTINCT FP.CUSTOMERID) AS SIX_MONTH_RETENTION_RATE
FROM FirstPurchase FP
LEFT JOIN Retention R
    ON FP.CUSTOMERID = R.CUSTOMERID
GROUP BY FP.FIRST_PRODUCT_CATEGORY
ORDER BY FP.FIRST_PRODUCT_CATEGORY;