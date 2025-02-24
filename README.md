# **Cohort Analysis** 
Cohort Analysis with SQL This repository contains a project for analyzing customer behavior using Cohort Analysis. The project leverages SQL for data querying and manipulation, making it efficient for handling large datasets and performing complex calculations.

### **Key Features :**
- **Data Preprocessing:** Clean and prepare transaction data for cohort analysis.

- **Cohort Identification:** Identify cohorts based on customer acquisition dates or other relevant time-based criteria.

- **Retention Analysis:** Calculate retention rates and analyze customer behavior over time.

- **Behavioral Insights:** Group customers into cohorts to understand patterns such as retention, churn, and lifetime value.

- **Actionable Insights:** Provide recommendations for improving customer retention and engagement strategies.

### **Technologies Used :**

- SQL
- MySQL
- Python

### **Dataset** :
The dataset used in this project is available in CSV format. You can download it [here](./sales_retail_II_cleaned.zip).

### **Database Setup** :
Create a database named 'COHORT_ANALYSIS'
- To set up the database for this project, run the following SQL commands:
  
```sql
CREATE DATABASE IF NOT EXISTS COHORT_ANALYSIS;
USE COHORT_ANALYSIS;
```
- The dataset is imported and set up using Python :
  
  ```python

  # Importing Libraries
  import pandas as pd
  import mysql.connector
  import os

  # Load the dataset
  csv_files = [('sales_retail_II_cleaned.csv','sales_retail_II_cleaned')]

  # Connect to the MySQL database
  conn = mysql.connector.connect(
    host='localhost',
    user='your_username',
    password='your_pasword',
    database='COHORT_ANALYSIS'
     )
   
   cursor = conn.cursor()

  # Folder containing the CSV files
  folder_path = 'Your_Folder_Path'

  def get_sql_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return 'INT'
    elif pd.api.types.is_float_dtype(dtype):
        return 'FLOAT'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'DATETIME'
    else:
        return 'TEXT'

  for csv_file, table_name in csv_files:
    file_path = os.path.join(folder_path, csv_file)
    
    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(file_path,encoding='iso-8859-1')
    
    # Replace NaN with None to handle SQL NULL
    df = df.where(pd.notnull(df), None)
    
    # Debugging: Check for NaN values
    print(f"Processing {csv_file}")
    print(f"NaN values before replacement:\n{df.isnull().sum()}\n")

    # Clean column names
    df.columns = [col.replace(' ', '_').replace('-', '_').replace('.', '_') for col in df.columns]

    # Generate the CREATE TABLE statement with appropriate data types
    columns = ', '.join([f'`{col}` {get_sql_type(df[col].dtype)}' for col in df.columns])
    create_table_query = f'CREATE TABLE IF NOT EXISTS `{table_name}` ({columns})'
    cursor.execute(create_table_query)

    # Insert DataFrame data into the MySQL table
    for _, row in df.iterrows():
        # Convert row to tuple and handle NaN/None explicitly
        values = tuple(None if pd.isna(x) else x for x in row)
        sql = f"INSERT INTO `{table_name}` ({', '.join(['`' + col + '`' for col in df.columns])}) VALUES ({', '.join(['%s'] * len(row))})"
        cursor.execute(sql, values)

    # Commit the transaction for the current CSV file
    conn.commit()

  # Close the connection
  conn.close()
  
  import mysql.connector

  db = mysql.connector.connect(host = 'localhost',
                            username = 'Your_username',
                            password = 'Your_password',
                            database = 'COHORT_ANALYSIS')
  cur = db.cursor()
  ```
## **Dataset Exploration :**
```sql
SELECT * FROM sales_retail_ii_cleaned
LIMIT 1000;
```
--Output--
| InvoiceNo | StockCode | Description                          | Quantity | InvoiceDate | UnitPrice | CustomerID | Country        |
|-----------|-----------|--------------------------------------|----------|-------------|-----------|------------|----------------|
| 536365    | 85123A    | WHITE HANGING HEART T-LIGHT HOLDER   | 6        | 12/1/2010   | 2.55      | 17850      | United Kingdom |
| 536365    | 71053     | WHITE METAL LANTERN                  | 6        | 12/1/2010   | 3.39      | 17850      | United Kingdom |
| 536365    | 84406B    | CREAM CUPID HEARTS COAT HANGER       | 8        | 12/1/2010   | 2.75      | 17850      | United Kingdom |
| 536365    | 22752     | SET 7 BABUSHKA NESTING BOXES         | 2        | 12/1/2010   | 7.65      | 17850      | United Kingdom |
| 536367    | 84879     | ASSORTED COLOUR BIRD ORNAMENT       | 32       | 12/1/2010   | 1.69      | 13047      | United Kingdom |
| 536367    | 22745     | POPPY'S PLAYHOUSE BEDROOM           | 6        | 12/1/2010   | 2.1       | 13047      | United Kingdom |
| 536370    | 22728     | ALARM CLOCK BAKELIKE PINK            | 24       | 12/1/2010   | 3.75      | 12583      | France         |
| 536370    | 22727     | ALARM CLOCK BAKELIKE RED             | 24       | 12/1/2010   | 3.75      | 12583      | France         |
| 536370    | 22726     | ALARM CLOCK BAKELIKE GREEN           | 12       | 12/1/2010   | 3.75      | 12583      | France         |
| 536370    | 21724     | PANDA AND BUNNIES STICKER SHEET      | 12       | 12/1/2010   | 0.85      | 12583      | France         |
| 536370    | 21883     | STARS GIFT TAPE                      | 24       | 12/1/2010   | 0.65      | 12583      | France         |
| 536370    | 10002     | INFLATABLE POLITICAL GLOBE           | 48       | 12/1/2010   | 0.85      | 12583      | France         |
| 536370    | 21791     | VINTAGE HEADS AND TAILS CARD GAME    | 24       | 12/1/2010   | 1.25      | 12583      | France         |
| 536370    | 21035     | SET/2 RED RETROSPOT TEA TOWELS       | 18       | 12/1/2010   | 2.95      | 12583      | France         |

## **Dataset Description :**

- **InvoiceNo :** Unique identifier for each invoice (order). This is similar to ORDERNUMBER in your example.

- **StockCode :** Unique identifier for each product. This can be used to track specific items in the inventory.

- **Description :** A brief description of the product. This provides details about the item being ordered.

- **Quantity :** The number of units ordered for each product. This is similar to QUANTITYORDERED in your example.

- **InvoiceDate :** The date when the invoice was generated (order date). This is similar to ORDERDATE_1 in your example.

- **UnitPrice :** The price per unit of the product. This is similar to PRICEEACH in your example.

- **CustomerID :** Unique identifier for each customer. This helps in tracking customer-specific orders.

- **Country :** The country where the customer is located. This provides geographical information about the order.

## Total Number of Records :

```sql
SELECT COUNT(*) as Total_records FROM sales_retail_ii_cleaned;
```
--Output--
| Total_records |
|---------------|
| 397924        |

- The dataset contains a total of 397,924 records in the sales_retail_ii_cleaned table. This large volume of data provides a robust foundation for detailed analysis, trend identification, and actionable insights into sales performance, customer behavior, and product performance.

## Checking Unique Values :

### Unique Identifier InvoiceNo :
```sql
select distinct InvoiceNo from sales_retail_ii_cleaned;
```
--Output--
| InvoiceNo |
|-----------|
| 536365    |
| 536366    |
| 536367    |
| 536368    |
| 536369    |
| 536370    |
| 536371    |
| 536372    |
| 536373    |
| 536374    |
| 536375    |
| 536376    |
| 536377    |
| 536378    |
| 536380    |

- The dataset contains unique invoice numbers, such as 536365, 536366, and 536367, which represent individual transactions. These unique identifiers are essential for analyzing transaction-level details, such as order frequency, customer purchasing patterns, and revenue trends.

### Unique StockCode :
```sql
select  StockCode from sales_retail_ii_cleaned;
```
--Output--
| StockCode |
|-----------|
| 85123A    |
| 71053     |
| 84406B    |
| 84029G    |
| 84029E    |
| 22752     |
| 21730     |

- The dataset includes unique stock codes such as 85123A, 71053, and 84406B, which represent individual products.

### Unique Description :
```sql
select  Description from sales_retail_ii_cleaned;
```
--Output--
| Description                          |
|--------------------------------------|
| WHITE HANGING HEART T-LIGHT HOLDER   |
| WHITE METAL LANTERN                  |
| CREAM CUPID HEARTS COAT HANGER       |
| KNITTED UNION FLAG HOT WATER BOTTLE  |
| RED WOOLLY HOTTIE WHITE HEART.       |
| SET 7 BABUSHKA NESTING BOXES         |

- These descriptions provide insights into the types of products sold, which appear to include decorative, seasonal, and gift items. This information can be used to analyze product popularity, customer preferences, and seasonal trends.

### Unique Quantity :
```sql
select  distinct Quantity from sales_retail_ii_cleaned;
```
--Output--
| Quantity |
|----------|
| 6        |
| 8        |
| 2        |
| 32       |
| 3        |
| 4        |
| 24       |
| 12       |
| 48       |
| 18       |
| 20       |
| 36       |
| 80       |
| 64       |
| 10       |
| 120      |

- The dataset includes quantities sold for various products, ranging from small quantities like 2 and 6 to larger bulk purchases like 120 and 80 and so more. This variation in quantities indicates diverse purchasing behaviors, from individual retail sales to wholesale or bulk orders.

### Unique InvoiceDate :
```sql
select  distinct InvoiceDate from sales_retail_ii_cleaned;
```
--Output--
| InvoiceDate |
|-------------|
| 01-12-10    |
| 02-12-10    |
| 03-12-10    |
| 05-12-10    |
| 04-01-11    |
| 05-01-11    |
| 06-01-11    |
| 07-01-11    |
| 09-01-11    |
| 10-01-11    |
| 11-01-11    |

- The dataset includes invoice dates ranging from December 2010 to January 2011, indicating a focus on the holiday season and early-year sales.

### Unique UnitPrice :
```sql
select  distinct UnitPrice from sales_retail_ii_cleaned;
```
--Output--
| UnitPrice |
|-----------|
| 2.55      |
| 3.39      |
| 2.75      |
| 7.65      |
| 4.25      |
| 1.85      |
| 1.69      |
| 2.1       |
| 3.75      |

- The dataset includes unit prices for products, ranging from 1.69 to 7.65 and so more. This variation in pricing reflects a diverse product portfolio, catering to different customer segments and price points.

### Unique CustomerID :
```sql
select  distinct CustomerID from sales_retail_ii_cleaned;
```
--Output--
| CustomerID |
|------------|
| 17850      |
| 13047      |
| 12583      |
| 13748      |
| 15100      |
| 15291      |

- The dataset includes unique Customer IDs, such as 17850, 13047, and 12583, which represent individual customers. These IDs are essential for analyzing customer behavior, purchase patterns, and segmentation for targeted marketing strategies.

### Unique Country :
```sql
select  distinct country from sales_retail_ii_cleaned;
```
--Output--
| country               |
|-----------------------|
| United Kingdom        |
| France                |
| Australia             |
| Netherlands           |
| Germany               |
| Norway                |
| EIRE                  |
| Switzerland           |
| Spain                 |
| Poland                |
| Portugal              |
| Italy                 |
| Belgium               |
| Lithuania             |
| Japan                 |
| Iceland               |
| Channel Islands       |
| Denmark               |
| Cyprus                |
| Sweden                |
| Finland               |
| Austria               |
| Greece                |
| Singapore             |
| Lebanon               |
| United Arab Emirates  |
| Israel                |
| Saudi Arabia          |
| Czech Republic        |
| Canada                |
| Unspecified           |
| Brazil                |
| USA                   |
| European Community    |
| Bahrain               |
| Malta                 |
| RSA                   |

- The dataset includes sales data from 36 countries, with the majority of transactions originating from the United Kingdom. Other significant markets include France, Germany, Australia, and Netherlands. This global presence highlights the business's international reach and provides opportunities for analyzing regional sales performance, identifying high-growth markets, and tailoring strategies to local preferences.

## Maximum Invoice Date :
```sql
SELECT MAX(INVOICEDATE) as MAX_INVOICEDATE FROM sales_retail_ii_cleaned;
```
--Output--
| MAX_INVOICEDATE |
|-----------------|
| 12/9/2011       |

- The latest invoice date in the dataset is December 9, 2011. This indicates that the dataset covers sales activity up to this date, providing a snapshot of the business's performance leading up to the end of 2011.

## Find the Customer Revenue :
```sql
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
```
--Output--
| INVOICEDATE | CUSTOMERID | REVENUE |
|-------------|------------|---------|
| 18-01-11    | 12346      | 77183.6 |
| 07-12-10    | 12347      | 25.2    |
| 07-12-10    | 12347      | 17      |
| 07-12-10    | 12347      | 39      |
| 07-12-10    | 12347      | 23.4    |
| 07-12-10    | 12347      | 15      |
| 07-12-10    | 12347      | 15      |
| 07-12-10    | 12347      | 15      |
| 07-12-10    | 12347      | 15      |
| 07-12-10    | 12347      | 15      |
| 07-12-10    | 12347      | 15      |
| 07-12-10    | 12347      | 15      |

- The dataset shows revenue generated by specific customers on specific dates:

1. Customer 12346 made a high-value purchase of $77,183.60 on January 18, 2011, likely a bulk or wholesale order.

2. Customer 12347 made multiple smaller transactions on December 7, 2010, with individual revenues ranging from 15 to 39, indicating 
   retail purchases.

3. This highlights the diversity in customer purchasing behavior, from high-value bulk orders to smaller, frequent transactions.

# Cohort Analysis :

## Find the First Purchase Date :
```sql
-- Find the first purchase date for each customer
SELECT CUSTOMERID,
MIN(INVOICEDATE) AS FIRST_PURCHASE_DATE
FROM sales_retail_ii_cleaned
GROUP BY CustomerID;
```
--Output--
| CUSTOMERID | FIRST_PURCHASE_DATE |
|------------|---------------------|
| 12346      | 18-01-11            |
| 12347      | 07-12-10            |
| 12348      | 16-12-10            |
| 12349      | 21-11-11            |
| 12350      | 02-02-11            |
| 12352      | 16-02-11            |
| 12353      | 19-05-11            |
| 12354      | 21-04-11            |
| 12355      | 09-05-11            |

- The dataset provides the first purchase dates for several customers, offering insights into their initial engagement with the business:

- **Customer 12346 :**

1. First purchase on January 18, 2011.

2. Likely a high-value or bulk purchase, as seen in previous data.

- **Customer 12347 :**

1. First purchase on December 7, 2010.

2. Engaged in multiple small transactions, typical of retail customers.

- **Other Customers :**

1. Customer 12348: First purchase on December 16, 2010.

2. Customer 12349: First purchase on November 21, 2011.

3. Customer 12350: First purchase on February 2, 2011.

4. Customer 12352: First purchase on February 16, 2011.

5. Customer 12353: First purchase on May 19, 2011.

6. Customer 12354: First purchase on April 21, 2011.

7. Customer 12355: First purchase on May 9, 2011.

- **Key Observations :**

1. **Diverse First Purchase Dates :** Customers joined at different times, indicating a steady flow of new customers throughout the year.

2. **Potential Seasonal Trends :**  Some customers, like 12347 and 12348, joined during the holiday season (December 2010), which might 
      indicate seasonal marketing effectiveness.

= **Actionable Steps :**

1. **Analyze Acquisition Channels :** Identify which marketing efforts or campaigns attracted these customers during their first purchase month.

2. **Seasonal Strategies :** Leverage insights from seasonal first purchases (e.g., holiday season) to optimize marketing campaigns.

3. **Customer Onboarding :** Improve onboarding processes for new customers to encourage repeat purchases and long-term loyalty.

## First Purchase Month :
```sql
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
```
--Output--
| InvoiceNo | CUSTOMERID | InvoiceDate | PURCHASE_MONTH | FIRST_PURCHASE_MONTH | REVENUE |
|-----------|------------|-------------|----------------|----------------------|---------|
| 541431    | 12346      | 18-01-11    | 01-01-11       | 01-01-11             | 77183.6 |
| 537626    | 12347      | 07-12-10    | 01-12-10       | 01-12-10             | 15      |
| 537626    | 12347      | 07-12-10    | 01-12-10       | 01-12-10             | 22.5    |
| 537626    | 12347      | 07-12-10    | 01-12-10       | 01-12-10             | 39      |
| 537626    | 12347      | 07-12-10    | 01-12-10       | 01-12-10             | 17      |
| 537626    | 12347      | 07-12-10    | 01-12-10       | 01-12-10             | 15      |
| 537626    | 12347      | 07-12-10    | 01-12-10       | 01-12-10             | 14.85   |
| 537626    | 12347      | 07-12-10    | 01-12-10       | 01-12-10             | 35.7    |
| 537626    | 12347      | 07-12-10    | 01-12-10       | 01-12-10             | 15      |
| 537626    | 12347      | 07-12-10    | 01-12-10       | 01-12-10             | 19.8    |
| 537626    | 12347      | 07-12-10    | 01-12-10       | 01-12-10             | 70.8    |
| 537626    | 12347      | 07-12-10    | 01-12-10       | 01-12-10             | 55.8    |
| 537626    | 12347      | 07-12-10    | 01-12-10       | 01-12-10             | 17      |
| 537626    | 12347      | 07-12-10    | 01-12-10       | 01-12-10             | 25.2    |
| 537626    | 12347      | 07-12-10    | 01-12-10       | 01-12-10             | 23.4    |
| 537626    | 12347      | 07-12-10    | 01-12-10       | 01-12-10             | 15      |
| 537626    | 12347      | 07-12-10    | 01-12-10       | 01-12-10             | 15      |
| 537626    | 12347      | 07-12-10    | 01-12-10       | 01-12-10             | 15      |
| 537626    | 12347      | 07-12-10    | 01-12-10       | 01-12-10             | 15      |
| 537626    | 12347      | 07-12-10    | 01-12-10       | 01-12-10             | 32.94   |

- The dataset highlights first-month purchase behavior for specific customers:

1. **Customer 12346 :** Made a single high-value purchase of $77,183.60 in their first month (January 2011).

    - This indicates a strong initial engagement, likely a bulk or wholesale order.

2. **Customer 12347 :** Made multiple smaller transactions in their first month (December 2010), with revenues ranging from
    14.85 to 39.

3. This suggests frequent, smaller purchases typical of retail customers.

- **Key Observations :**

**High-Value First Purchases :** Customer 12346's high-value first purchase indicates potential for long-term loyalty and significant revenue contribution.

**Frequent First Purchases :** Customer 12347's multiple small transactions in their first month suggest a retail-focused buying pattern.

- **Actionable Steps :**

**High-Value First Purchases :** Nurture these customers with personalized follow-ups, loyalty programs, or exclusive offers to encourage repeat purchases.

**Frequent First Purchases :** Implement strategies to increase order values, such as upselling, cross-selling, or bundling products.

## The first and second purchase dates for each customer :
```sql
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
```
--Output--
| CUSTOMERID | FIRST_PURCHASE_DATE | SECOND_PURCHASE_DATE |
|------------|---------------------|----------------------|
| 12346      | 18-01-11            | NULL                 |
| 12347      | 07-12-10            | 07-12-10             |
| 12348      | 16-12-10            | 16-12-10             |
| 12349      | 21-11-11            | 21-11-11             |
| 12350      | 02-02-11            | 02-02-11             |
| 12352      | 16-02-11            | 16-02-11             |
| 12353      | 19-05-11            | 19-05-11             |
| 12354      | 21-04-11            | 21-04-11             |
| 12355      | 09-05-11            | 09-05-11             |

- The dataset reveals the first and second purchase dates for several customers, providing insights into their purchasing behavior:

1. Customer 12346:

**First Purchase:** January 18, 2011.

**Second Purchase:** NULL (no second purchase recorded).

- This customer made only one high-value purchase, indicating a potential one-time buyer or bulk purchaser.

- Other Customers (12347 to 12355):

- First and Second Purchase Dates: The same date for both purchases.

- This suggests these customers made multiple transactions on their first day of engagement, likely purchasing multiple items in a single shopping session.

- **Key Observations:**

1. **Single Purchase Behavior:** Customer 12346 did not make a second purchase, highlighting a potential area for re-engagement strategies.

2. **Multiple Transactions on First Day:** Customers like 12347 to 12355 made multiple purchases on their first day, indicating strong initial engagement.

- **Actionable Steps:**
  
1. **Re-Engage One-Time Buyers:** For customers like 12346, implement win-back campaigns or personalized offers to encourage repeat 
    purchases.

2. **Leverage High Initial Engagement:** For customers with multiple transactions on their first day, focus on upselling, cross-selling, 
     or loyalty programs to maximize their lifetime value.

3. **Analyze Purchase Patterns:** Investigate why some customers make multiple purchases on their first day and replicate successful 
     strategies for other customers.

## The start and end of the second month after the first purchase :
```sql
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
```
--Output--
| CUSTOMERID | START_SECOND_PURCHASE_DATE | END_SECOND_PURCHASE_DATE  |
|------------|----------------------------|-------------------------- |
| 12346      | 18-02-11                    | 18-03-11                 |
| 12347      | 07-01-11                    | 07-02-11                 |
| 12348      | 16-01-11                    | 16-02-11                 |
| 12349      | 21-12-11                    | 21-01-12                 |
| 12350      | 02-03-11                    | 02-04-11                 |
| 12352      | 16-03-11                    | 16-04-11                 |
| 12353      | 19-06-11                    | 19-07-11                 |
| 12354      | 21-05-11                    | 21-06-11                 |
| 12355      | 09-06-11                    | 09-07-11                 |
| 12356      | 18-02-11                    | 18-03-11                 |
| 12357      | 06-12-11                    | 06-01-12                 |
| 12358      | 12-08-11                    | 12-09-11                 |
| 12359      | 12-02-11                    | 12-03-11                 |
| 12360      | 23-06-11                    | 23-07-11                 |
| 12361      | 25-03-11                    | 25-04-11                 |
| 12362      | 17-03-11                    | 17-04-11                 |

- The dataset provides the start and end dates for the second purchase window for each customer, indicating the time frame during which they could make their second purchase:

**Customer 12346:**

**Second Purchase Window:** February 18, 2011, to March 18, 2011.

- No second purchase was made, as previously noted.

**Other Customers:**

**Second Purchase Window:** Typically spans one month after the first purchase date.

- For example:

Customer 12347: January 7, 2011, to February 7, 2011.

Customer 12348: January 16, 2011, to February 16, 2011.

Customer 12349: December 21, 2011, to January 21, 2012.

- **Key Observations:**

1. **One-Month Window:** The second purchase window is consistently one month long, suggesting a standardized approach to measuring 
        customer re-engagement.

2. **Potential for Re-Engagement:** Customers who did not make a second purchase within this window (e.g., 12346) represent 
         opportunities for targeted re-engagement campaigns.

- **Actionable Steps:**
  
1. **Re-Engage Inactive Customers:** For customers who did not make a second purchase within the window, implement win-back campaigns, 
    such as personalized offers or discounts.

2. **Optimize Timing:** Analyze whether the one-month window is optimal for re-engagement or if adjustments (e.g., shorter or longer 
      windows) could improve results.

3. **Segment Customers:** Group customers based on their second purchase behavior (e.g., those who made a second purchase vs. those who 
        did not) to tailor marketing strategies.


