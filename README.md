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
| 09-12-11        |

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

## Identify customers who made a purchase in the second month :
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
```
--Output--
| CUSTOMERID | Sec_M_PURCHASE_DATE|
|------------|--------------|
| 14606      | 01-01-11     |
| 17841      | 01-01-11     |
| 14210      | 03-01-11     |
| 14180      | 02-01-11     |
| 16029      | 01-01-11     |
| 16725      | 03-01-11     |
| 15235      | 01-01-11     |
| 13694      | 01-01-11     |
| 13798      | 02-01-11     |
| 12921      | 01-01-11     |
| 15311      | 01-01-11     |
| 15769      | 03-01-11     |
| 13089      | 05-01-11     |
| 13715      | 02-01-11     |

- The dataset provides the second purchase dates for several customers, with most purchases occurring in early January 2011:

- **Key Dates:**

- The majority of second purchases happened on January 1, 2011, indicating a strong start to the new year.

- Other purchases were made on January 2, 3, and 5, 2011, suggesting continued engagement in the first week of January.

- **Customer Behavior:**

- Customers like 14606, 17841, and 16029 made their second purchase on January 1, 2011, showing immediate re-engagement.

- Customers like 13089 made their second purchase on January 5, 2011, indicating a slightly delayed but still prompt re-engagement.

## Identify customers who made a purchase in the third month :

**A join quarter is a concept used in customer segmentation and RFM (Recency, Frequency, Monetary) analysis. It refers to the quarter (3-month period) in which a customer made their first purchase or joined the business. This metric helps in understanding customer behavior over time and segmenting customers based on their acquisition period.**

```sql
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
```
--Output--
| CUSTOMERID | JOIN_QUARTER |
|------------|-------------|
| 14911      | 4           |
| 14496      | 4           |
| 17511      | 4           |
| 14312      | 1           |
| 17800      | 1           |
| 17409      | 2           |
| 13752      | 2           |
| 17405      | 3           |
| 16764      | 3           |

- The dataset provides the join quarters for several customers, indicating the quarter in which they made their first purchase:

**Customers Joining in Q4:**

- 14911, 14496, and 17511 joined in Q4 (October–December).

- This could indicate a strong holiday season effect, as Q4 often includes year-end and festive shopping periods.

**Customers Joining in Q1:**

- 14312 and 17800 joined in Q1 (January–March).

- This might reflect New Year promotions or post-holiday sales.

**Customers Joining in Q2 and Q3:**

- 17409 and 13752 joined in Q2 (April–June).

- 17405 and 16764 joined in Q3 (July–September).

- These quarters may represent steady customer acquisition outside of peak holiday seasons.

- **Key Observations:**
  
1. **Seasonal Trends:** Higher customer acquisition in Q4 suggests the holiday season drives new customer engagement.

2. **Year-Round Acquisition:** Customers joining in Q1, Q2, and Q3 indicate consistent acquisition efforts throughout the year.

## The percentage of customers who made a purchase in the second month :
```sql
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
```
--Output--
| PercentageRetained |
|--------------------|
| 23.16202           |

- A retention rate of 23.16% indicates that approximately 1 in 4 customers continued their relationship with the business.
- This suggests room for improvement in retaining customers and reducing churn.

## Cohort Month :
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
```
--Output--
| CUSTOMERID | FIRST_PURCHASE_MONTH | COHORT_MONTH |
|------------|----------------------|-------------|
| 12346      | 01-01-11             | MONTH_0     |
| 12347      | 01-12-10             | MONTH_0     |
| 12347      | 01-12-10             | MONTH_0     |
| 12347      | 01-12-10             | MONTH_0     |
| 12347      | 01-12-10             | MONTH_1     |
| 12347      | 01-12-10             | MONTH_1     |
| 12347      | 01-12-10             | MONTH_1     |
| 12347      | 01-12-10             | MONTH_1     |
| 12347      | 01-12-10             | MONTH_1     |
| 12347      | 01-12-10             | MONTH_1     |
| 12347      | 01-12-10             | MONTH_4     |
| 12347      | 01-12-10             | MONTH_4     |
| 12347      | 01-12-10             | MONTH_4     |
| 12347      | 01-12-10             | MONTH_6     |
| 12347      | 01-12-10             | MONTH_6     |
| 12347      | 01-12-10             | MONTH_6     |
| 12347      | 01-12-10             | MONTH_8     |
| 12347      | 01-12-10             | MONTH_8     |
| 12347      | 01-12-10             | MONTH_8     |

- The dataset provides a cohort analysis for customers based on their first purchase month and subsequent engagement over time:

**Customer 12346:**

**First Purchase Month:** January 2011 (MONTH_0).

- No further purchases recorded, indicating this customer did not return after their initial purchase.

**Customer 12347:**

- First Purchase Month: December 2010 (MONTH_0).

- Made multiple purchases in subsequent months:

MONTH_1: January 2011.

MONTH_4: April 2011.

MONTH_6: June 2011.

MONTH_8: August 2011.

- This customer shows consistent re-engagement over time, indicating strong loyalty.

- **Key Observations:**
  
- **Retention Patterns:**

- Customer 12347 demonstrates a pattern of repeat purchases, with activity in months 1, 4, 6, and 8 after their first purchase.

- Customer 12346, on the other hand, did not make any repeat purchases, representing a churned customer.

- **Cohort Analysis:** Tracking customer behavior over time (e.g., MONTH_0, MONTH_1, etc.) helps identify retention trends and the effectiveness of engagement strategies.

- **Actionable Steps:**

1. **Retain Loyal Customers:** For customers like 12347, implement loyalty programs or exclusive offers to maintain their engagement.

2. **Re-Engage Churned Customers:** For customers like 12346, develop win-back campaigns, such as personalized discounts or reminders, to encourage repeat purchases.

3. **Analyze Cohort Performance:** Compare retention rates across different cohorts (e.g., customers who joined in different months) to identify trends and optimize strategies.

## Count the number of repeat purchases for each customer :
```sql
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
```
--Output--
| CUSTOMERID | COHORT_MONTH | REPEAT_PURCHASE_COUNT |
|------------|-------------|----------------------|
| 12347      | 12-2010     | 150                  |
| 12348      | 12-2010     | 13                   |
| 12352      | 02-2011     | 69                   |
| 12356      | 01-2011     | 22                   |
| 12358      | 07-2011     | 6                    |
| 12359      | 01-2011     | 231                  |
| 12360      | 05-2011     | 83                   |
| 12362      | 02-2011     | 238                  |
| 12363      | 04-2011     | 6                    |
| 12364      | 08-2011     | 50                   |
| 12370      | 12-2010     | 83                   |
| 12371      | 10-2011     | 0                    |
| 12372      | 02-2011     | 31                   |
| 12375      | 09-2011     | 5                    |
| 12377      | 12-2010     | 33                   |
| 12379      | 06-2011     | 19                   |
| 12380      | 06-2011     | 67                   |
| 12381      | 08-2011     | 24                   |
| 12383      | 12-2010     | 61                   |

- The dataset provides repeat purchase counts for customers, grouped by their cohort month (the month of their first purchase). This highlights customer loyalty and engagement over time:

1. **Top Performers:**

**Customer 12359:** Made 231 repeat purchases after joining in January 2011.

**Customer 12362:** Made 238 repeat purchases after joining in February 2011.

- These customers demonstrate exceptional loyalty and high engagement.

2. **Moderate Performers:**

**Customer 12347:** Made 150 repeat purchases after joining in December 2010.

**Customer 12360:** Made 83 repeat purchases after joining in May 2011.

- These customers show consistent but moderate engagement.

3. **Low Performers:**

**Customer 12371:** Made 0 repeat purchases after joining in October 2011.

**Customer 12363:** Made only 6 repeat purchases after joining in April 2011.

- These customers represent opportunities for re-engagement.

- **Key Observations:**
  
1. **High Engagement:** Customers like 12359 and 12362 are highly valuable and should be nurtured through loyalty programs or exclusive 
     offers.

2. **Low Engagement:** Customers like 12371 and 12363 may require targeted campaigns to re-engage them.

3. **Cohort Trends:** Customers who joined in December 2010 and January–February 2011 show higher repeat purchase counts, possibly due to effective onboarding or seasonal promotions.

- **Actionable Steps:**
  
1. **Reward Loyal Customers:** Offer exclusive perks, discounts, or early access to new products for high-performing customers like 12359 and 12362.

2. **Re-Engage Low Performers:** Implement win-back campaigns, such as personalized offers or surveys, to understand and address their lack of engagement.

3. **Analyze Cohort Performance:** Investigate why certain cohorts (e.g., December 2010 and January–February 2011) show higher engagement and replicate successful strategies.

## Distribution of Repeat Purchases :
```sql
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
```
--Output--

| COHORT_MONTH | REPEAT_PURCHASE_COUNT | CUSTOMER_COUNT |
|-------------|----------------------|---------------|
| 2010-12     | 0                    | 9             |
| 2010-12     | 1                    | 7             |
| 2010-12     | 2                    | 5             |
| 2010-12     | 3                    | 6             |
| 2010-12     | 4                    | 4             |
| 2010-12     | 5                    | 3             |
| 2010-12     | 6                    | 5             |
| 2010-12     | 7                    | 4             |
| 2010-12     | 8                    | 8             |
| 2010-12     | 9                    | 7             |
| 2010-12     | 10                   | 4             |
| 2010-12     | 11                   | 4             |
| 2010-12     | 12                   | 3             |
| 2010-12     | 13                   | 6             |
| 2010-12     | 18                   | 10            |
| 2010-12     | 19                   | 8             |
| 2010-12     | 20                   | 10            |
| 2010-12     | 21                   | 9             |
| 2010-12     | 22                   | 6             |
| 2010-12     | 23                   | 7             |
| 2010-12     | 24                   | 2             |
| 2010-12     | 25                   | 4             |
| 2010-12     | 26                   | 5             |
| 2011-01     | 490                  | 1             |
| 2011-01     | 495                  | 1             |
| 2011-01     | 559                  | 1             |
| 2011-01     | 580                  | 1             |
| 2011-01     | 669                  | 1             |
| 2011-01     | 698                  | 1             |
| 2011-01     | 804                  | 1             |
| 2011-01     | 832                  | 1             |
| 2011-01     | 947                  | 1             |
| 2011-01     | 1637                 | 1             |
| 2011-02     | 0                    | 4             |
| 2011-02     | 1                    | 5             |
| 2011-02     | 2                    | 5             |
| 2011-02     | 3                    | 5             |
| 2011-02     | 4                    | 4             |
| 2011-02     | 5                    | 3             |
| 2011-02     | 7                    | 2             |
| 2011-02     | 8                    | 8             |
| 2011-02     | 9                    | 10            |
| 2011-02     | 10                   | 4             |
| 2011-03     | 330                  | 1             |
| 2011-03     | 343                  | 1             |
| 2011-03     | 352                  | 1             |

- The dataset provides a cohort-based analysis of repeat purchase behavior, showing the distribution of repeat purchases for customers who joined in specific months.

- **Key Observations:**
  
1. **December 2010 Cohort:**

- **Repeat Purchase Distribution:** Most customers made 0–26 repeat purchases, with the majority falling in the 0–10 range. A smaller 
   group made 18–26 repeat purchases, indicating moderate to high engagement.

- **Customer Count:**

 1. 9 customers made 0 repeat purchases, representing churned customers.

2. 10 customers made 18–20 repeat purchases, showing strong loyalty.

2. **January 2011 Cohort:**

- **Repeat Purchase Distribution:** A few customers made exceptionally high repeat purchases (e.g., 1637, 947, 832). These outliers 
  represent highly engaged, loyal customers.

- **Customer Count:** Each high-repeat customer is unique, indicating a small but highly valuable segment.

3. **February 2011 Cohort:**

- **Repeat Purchase Distribution:** Similar to December 2010, most customers made 0–10 repeat purchases. A smaller group made 8–10 
    repeat purchases, showing moderate engagement.

 - **Customer Count:**

1. 4 customers made 0 repeat purchases, indicating churn.

2. 10 customers made 9 repeat purchases, representing a loyal segment.

4. **March 2011 Cohort:**

- **Repeat Purchase Distribution:** A few customers made 330–352 repeat purchases, indicating high engagement.

- **Customer Count:** Each high-repeat customer is unique, similar to the January 2011 cohort.

- **Actionable Insights:**

 1. **High-Engagement Customers:** Customers with hundreds of repeat purchases (e.g., 1637, 947, 832) are highly valuable.

**Action:** Reward these customers with loyalty programs, exclusive offers, or personalized experiences to maintain their engagement.

2. **Moderate-Engagement Customers:** Customers with 8–26 repeat purchases (e.g., 18–26 in December 2010, 8–10 in February 2011) 
     represent a loyal but under-optimized segment.

**Action:** Implement targeted upselling or cross-selling strategies to increase their purchase frequency.

3. **Churned Customers:** Customers with 0 repeat purchases (e.g., 9 in December 2010, 4 in February 2011) represent lost revenue opportunities.

**Action:** Launch win-back campaigns, such as personalized discounts or surveys, to understand and address their reasons for churn.

4. **Cohort-Specific Strategies:**

- December 2010 and February 2011 Cohorts: Focus on improving retention and increasing repeat purchases for the moderate-engagement 
  group.

- January and March 2011 Cohorts: Leverage the high-engagement customers to drive advocacy and referrals.

## Analyze the frequency of repeat purchases for each cohort :
```sql
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
```
--Output--
| COHORT_MONTH | AVG_REPEAT_PURCHASES |
|-------------|----------------------|
| 2010-12     | 192.8497             |
| 2011-01     | 109.1209             |
| 2011-02     | 74.2474              |
| 2011-03     | 71.6928              |
| 2011-04     | 61.2915              |
| 2011-05     | 50.2368              |
| 2011-06     | 54.2614              |
| 2011-07     | 53.1835              |
| 2011-08     | 91.4778              |
| 2011-09     | 45.5461              |
| 2011-10     | 44.6695              |
| 2011-11     | 26.9565              |

- The dataset provides the average repeat purchases for customers grouped by their cohort month (the month of their first purchase). This highlights trends in customer engagement and loyalty over time:

- **Key Observations :**
  
1. **Highest Engagement :**

**December 2010 Cohort :**

**Average Repeat Purchases :** 192.85

- This cohort shows the highest engagement, likely due to holiday season promotions or effective onboarding strategies.

2. **Moderate Engagement :**

**January 2011 Cohort :**

**Average Repeat Purchases :** 109.12

**August 2011 Cohort :**

**Average Repeat Purchases :** 91.48

- These cohorts demonstrate strong but slightly lower engagement compared to December 2010.

**Lower Engagement :**

**November 2011 Cohort :**

**Average Repeat Purchases :** 26.96

**October 2011 Cohort :**

**Average Repeat Purchases :** 44.67

- These cohorts show the lowest engagement, indicating potential issues with retention or customer satisfaction during this period.

**General Trend :** Engagement tends to decline over the year, with the highest averages in December 2010 and the lowest in November 2011.

- **Actionable Insights :**
  
1. **Leverage High-Engagement Cohorts :** Analyze what made the December 2010 and January 2011 cohorts so successful (e.g., holiday promotions, marketing campaigns) and replicate these strategies.

2. **Improve Low-Engagement Cohorts :** Investigate why the November 2011 and October 2011 cohorts have lower engagement. Possible reasons include ineffective onboarding, lack of follow-up, or seasonal factors.

3. **Targeted Retention Strategies :** For cohorts with moderate engagement (e.g., February–August 2011), implement targeted retention strategies, such as personalized offers or loyalty programs, to increase repeat purchases.

4. **Seasonal Adjustments :** If the decline in engagement is seasonal, plan ahead by optimizing marketing and retention efforts for weaker months (e.g., November).

## Determine the average number of months between a customer's first and last purchase for each cohort :
```sql
-- determine the average number of months between a customer's first and last purchase for each cohort.

WITH FirstPurchase AS (
    -- Step 1: Find the first purchase date for each customer
    SELECT 
        CUSTOMERID,
        MIN(INVOICEDATE) AS FIRST_PURCHASE_DATE,
        DATE_FORMAT(MIN(INVOICEDATE), '%Y-%m') AS COHORT_MONTH
    FROM SALES_RETAIL_II_CLEANED
    GROUP BY CUSTOMERID
),

LastPurchase AS (
    -- Step 2: Find the last purchase date for each customer
    SELECT 
        CUSTOMERID,
        MAX(INVOICEDATE) AS LAST_PURCHASE_DATE
    FROM SALES_RETAIL_II_CLEANED
    GROUP BY CUSTOMERID
),

CustomerLifespan AS (
    -- Step 3: Calculate the number of months between the first and last purchase for each customer
    SELECT 
        FP.CUSTOMERID,
        FP.COHORT_MONTH,
        TIMESTAMPDIFF(MONTH, FP.FIRST_PURCHASE_DATE, LP.LAST_PURCHASE_DATE) AS LIFESPAN_MONTHS
    FROM FirstPurchase FP
    JOIN LastPurchase LP
        ON FP.CUSTOMERID = LP.CUSTOMERID
)

-- Step 4: Calculate the average lifespan for each cohort
SELECT 
    COHORT_MONTH,
    AVG(LIFESPAN_MONTHS) AS AVG_LIFESPAN_MONTHS
FROM CustomerLifespan
GROUP BY COHORT_MONTH
ORDER BY COHORT_MONTH;
```
--Output--
| COHORT_MONTH | AVG_LIFESPAN_MONTHS |
|-------------|----------------------|
| 2010-12     | 8.3853               |
| 2011-01     | 6.4676               |
| 2011-02     | 5.3053               |
| 2011-03     | 4.2788               |
| 2011-04     | 3.4233               |
| 2011-05     | 2.9507               |
| 2011-06     | 2.4463               |
| 2011-07     | 1.5904               |
| 2011-08     | 1.142                |
| 2011-09     | 0.6689               |
| 2011-10     | 0.2207               |
| 2011-11     | 0.0309               |
| 2011-12     | 0                    |

- The dataset provides the average lifespan (in months) for customers grouped by their cohort month (the month of their first purchase). This metric reflects how long customers remain engaged with the business after their initial purchase.

- **Key Observations :**

1. **Longest Lifespan :**

 **December 2010 Cohort :**

**Average Lifespan :** 8.39 months

- This cohort has the longest engagement, likely due to effective onboarding, holiday season promotions, or strong customer retention 
  strategies.

**Declining Lifespan Over Time :**

- The average lifespan decreases steadily from December 2010 to December 2011:

**January 2011 :** 6.47 months

**February 2011 :** 5.31 months

**March 2011 :** 4.28 months

**November 2011 :** 0.03 months

**December 2011 :** 0 months (customers churned immediately).

2. **Lowest Lifespan :**

**November and December 2011 Cohorts :**

**Average Lifespan :** 0.03 months and 0 months, respectively.

- These cohorts show almost no retention, indicating significant issues with customer satisfaction or engagement during this period.

**Actionable Insights :**

1. **Leverage Success of December 2010 Cohort :** Analyze what made the December 2010 cohort so successful (e.g., holiday promotions, onboarding process) and replicate these strategies for other cohorts.

2. **Address Declining Lifespan :** Investigate why customer lifespans declined over the year.

  
3. **Possible reasons include :**

- Changes in product offerings or pricing.

- Ineffective marketing or retention strategies.

- Seasonal factors impacting customer behavior.

4. **Improve Retention for Later Cohorts :** For cohorts with short lifespans (e.g., November and December 2011), implement targeted retention strategies, such as:

- Personalized follow-ups after the first purchase.

- Loyalty programs or discounts for repeat purchases.

- Surveys to understand and address customer dissatisfaction.

5. **Seasonal Adjustments :** If the decline is seasonal, plan ahead by optimizing marketing and retention efforts for weaker months (e.g., November and December).

## Cross-Cohort Comparison:
### The activity of each customer by month :
```sql
-- Cross-Cohort Comparison:
-- How would you compare the retention rates of two different 
-- cohorts (e.g., customers who joined in January vs. February)?
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
```
--Output--
| CUSTOMERID | ACTIVITY_MONTH |
|------------|----------------|
| 17850      | 2010-12        |
| 13047      | 2010-12        |
| 12583      | 2010-12        |
| 13748      | 2010-12        |
| 15100      | 2010-12        |

### How would you compare the retention rates of two different cohorts (e.g., customers who joined in January vs. February):
```sql
-- Cross-Cohort Comparison:
-- How would you compare the retention rates of two different 
-- cohorts (e.g., customers who joined in January vs. February)?
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
```
--Output--

| COHORT_MONTH | ACTIVITY_MONTH | RETENTION_RATE |
|--------------|----------------|----------------|
| 2011-01      | 2011-01        | 100            |
| 2011-01      | 2011-02        | 100            |
| 2011-01      | 2011-03        | 100            |
| 2011-01      | 2011-04        | 100            |
| 2011-01      | 2011-05        | 100            |
| 2011-01      | 2011-06        | 100            |
| 2011-01      | 2011-07        | 100            |
| 2011-01      | 2011-08        | 100            |
| 2011-01      | 2011-09        | 100            |
| 2011-01      | 2011-10        | 100            |
| 2011-01      | 2011-11        | 100            |
| 2011-01      | 2011-12        | 100            |
| 2011-02      | 2011-02        | 100            |
| 2011-02      | 2011-03        | 100            |
| 2011-02      | 2011-04        | 100            |
| 2011-02      | 2011-05        | 100            |
| 2011-02      | 2011-06        | 100            |
| 2011-02      | 2011-07        | 100            |
| 2011-02      | 2011-08        | 100            |
| 2011-02      | 2011-09        | 100            |
| 2011-02      | 2011-10        | 100            |
| 2011-02      | 2011-11        | 100            |
| 2011-02      | 2011-12        | 100            |

- The dataset shows the retention rates for two cohorts: January 2011 and February 2011. Both cohorts maintain a 100% retention rate throughout the year, indicating that all customers who joined in these months remained active for the entire 12-month period.

**Key Observations :**

1. **Perfect Retention :** Both cohorts show 100% retention in every month after their join date.

2. This is highly unusual and suggests either: Exceptional customer retention strategies.

3.  A data anomaly or specific business context (e.g., subscription-based model with no churn).

4. **Comparison Between Cohorts :**

- **January 2011 Cohort :** Retained 100% of customers from January to December 2011.

- **February 2011 Cohort :** Retained 100% of customers from February to December 2011.

- Both cohorts performed equally well, with no difference in retention rates.

**Actionable Insights :**

1. **Leverage Successful Strategies :** Analyze what made these cohorts so successful (e.g., onboarding process, product quality, customer support) and replicate these strategies for other cohorts.

2. **Investigate Data Accuracy :** A 100% retention rate is rare. Verify the data to ensure it accurately reflects customer behavior.

3. **Compare with Other Cohorts :** Compare these cohorts with others (e.g., December 2010 or March 2011) to identify differences in retention strategies or customer behavior.

4. **Focus on Long-Term Engagement :** Since retention is already perfect, focus on increasing customer lifetime value (CLV) through upselling, cross-selling, or loyalty programs.

## Segment customers into cohorts based on their first purchased product category and calculate their 6-month retention rates :
```sql
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
```
--Output--

| FIRST_PRODUCT_CATEGORY | SIX_MONTH_RETENTION_RATE |
|------------------------|--------------------------|
| 10125                  | 100                      |
| 10135                  | 100                      |
| 15034                  | 0                        |
| 15036                  | 70                       |
| 15039                  | 66.66667                 |
| 15044C                 | 100                      |
| 15044D                 | 66.66667                 |
| 15056BL                | 50                       |
| 15056N                 | 37.5                     |
| 15056P                 | 50                       |
| 15058B                 | 0                        |
| 15058C                 | 0                        |
| 15060B                 | 0                        |
| 16014                  | 75                       |

- This dataset provides the six-month retention rates for customers based on their first product category. Retention rates vary significantly across categories, highlighting differences in customer engagement and loyalty.

- **Key Observations :**

1. **High Retention (100%) :** 
- **10125, 10135, and 15044C**: These product categories have a 100% retention rate, indicating strong customer satisfaction and loyalty.

2. **Moderate Retention (50–75%) :**
- **15036**: 70% retention.
- **15039**: 66.67% retention.
- **15044D**: 66.67% retention.
- **16014**: 75% retention.

These categories show decent retention but have room for improvement.

3. **Low Retention (0–50%) :**
- **15034, 15058B, 15058C, and 15060B**: 0% retention.
- **15056N**: 37.5% retention.
- **15056BL and 15056P**: 50% retention.

These categories struggle with customer retention, indicating potential issues with product quality, satisfaction, or relevance.

- **Actionable Insights :**

**Leverage High-Retention Categories :** 
- Analyze what makes categories like **10125, 10135, and 15044C** successful (e.g., product quality, customer support) and replicate these strategies for other categories.

**Improve Moderate-Retention Categories :**
- For categories like **15036, 15039, and 16014**, implement targeted retention strategies, such as personalized follow-ups or loyalty programs.

**Address Low-Retention Categories :**
- Investigate why categories like **15034, 15058B, and 15060B** have 0% retention. Possible reasons include:
  - Poor product quality or mismatch with customer expectations.
  - Lack of post-purchase engagement or support.
- Conduct surveys or feedback sessions to identify and address pain points.

**Product Optimization :**
- For categories with low retention, consider improving product features, pricing, or marketing to better align with customer needs.

## Percentiles for spending Group :
```sql
-- create cohorts based on customer behavior, such as high-spending vs. low-spending customers?
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
```
--Output--

| CUSTOMERID | TOTAL_SPENDING | SPENDING_GROUP |
|------------|----------------|----------------|
| 14646      | 280206         | 1              |
| 18102      | 259657         | 1              |
| 17450      | 194551         | 1              |
| 16446      | 168472         | 1              |
| 15392      | 1536           | 2              |
| 12877      | 1536           | 2              |
| 15625      | 1534           | 2              |
| 13107      | 1532           | 2              |
| 14236      | 491            | 3              |
| 15000      | 491            | 3              |
| 18074      | 490            | 3              |
| 14416      | 490            | 3              |
| 12809      | 489            | 4              |
| 15165      | 488            | 4              |
| 18249      | 95             | 5              |
| 16387      | 94             | 5              |
| 15397      | 94             | 5              |

- This dataset categorizes customers into spending groups based on their total spending, revealing distinct patterns in customer behavior.

- **Key Observations :**

**High-Spending Customers (Group 1)**
- **Total Spending**: $168,472 to $280,206.
- **Examples**: Customers 14646, 18102, 17450, and 16446.
- These customers are the most valuable, contributing significantly to revenue.

**Moderate-Spending Customers (Group 2)**
- **Total Spending**: $1,532 to $1,536.
- **Examples**: Customers 15392, 12877, 15625, and 13107.
- These customers show moderate engagement and represent a stable revenue stream.

**Low-Spending Customers (Groups 3–5)**
- **Group 3**: $490 to $491 (e.g., Customers 14236, 15000, 18074, 14416).
- **Group 4**: $488 to $489 (e.g., Customers 12809, 15165).
- **Group 5**: $94 to $95 (e.g., Customers 18249, 16387, 15397).
- These customers contribute minimally to revenue and may represent opportunities for upselling or re-engagement.

- **Actionable Insights**

**High-Spending Customers (Group 1)**
- **Action**: Reward and nurture these customers with exclusive perks, loyalty programs, or personalized offers to maintain their loyalty.

**Moderate-Spending Customers (Group 2)**
- **Action**: Implement upselling or cross-selling strategies to increase their spending and move them into the high-spending group.

**Low-Spending Customers (Groups 3–5)**
- **Action**:
  - For **Groups 3 and 4**, focus on increasing engagement through targeted campaigns or product recommendations.
  - For **Group 5**, investigate reasons for low spending (e.g., lack of interest, dissatisfaction) and address them through personalized outreach or incentives.

## Compare the 3-month retention rates of customers who joined in Q1 vs. Q2. :
```sql
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
```
--Output--
## Three Month Retention Rate by Join Quarter

| JOIN_QUARTER | THREE_MONTH_RETENTION_RATE |
|--------------|----------------------------|
| 1            | 25.22018                   |
| 2            | 16.94915                   |

- This dataset provides the three-month retention rates for customers grouped by their join quarter, revealing insights into customer engagement and loyalty trends.

- **Key Observations :**

1. **Q1 (Quarter 1)**
- **Retention Rate**: 25.22%
- This indicates that approximately 25% of customers who joined in Q1 remained active after three months.

2. **Q2 (Quarter 2)**
- **Retention Rate**: 16.95%
- This indicates that approximately 17% of customers who joined in Q2 remained active after three months.

3. **Higher Retention in Q1**
- Customers who joined in Q1 have a higher retention rate (25.22%) compared to those who joined in Q2 (16.95%).
- This could be due to factors such as New Year promotions, post-holiday engagement, or effective onboarding strategies.

4. **Lower Retention in Q2**
- The drop in retention for Q2 suggests potential issues with customer engagement or satisfaction during this period.

**Actionable Insights :**

1. **Leverage Q1 Success**
- Analyze what made Q1 successful (e.g., marketing campaigns, product launches) and replicate these strategies for other quarters.

2. **Improve Q2 Retention**
- Investigate why retention is lower in Q2. Possible reasons include:
  - Seasonal factors (e.g., fewer promotions or events).
  - Changes in customer behavior or preferences.
  - Ineffective onboarding or follow-up strategies.

3. **Targeted Retention Strategies**
- Implement targeted retention campaigns for Q2 customers, such as personalized offers, loyalty programs, or surveys to address dissatisfaction.

## Create cohorts based on customer behavior, such as high-spending vs. low-spending customers :
```sql
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
```
--Output--
| CUSTOMERID | TOTAL_SPENDING | SPENDING_COHORT  |
|------------|---------------|------------------|
| 14646      | 280206        | HIGH_SPENDING   |
| 18102      | 259657        | HIGH_SPENDING   |
| 17450      | 194551        | HIGH_SPENDING   |
| 14050      | 746           | MEDIUM_SPENDING |
| 14790      | 745           | MEDIUM_SPENDING |
| 13950      | 745           | MEDIUM_SPENDING |
| 13236      | 745           | MEDIUM_SPENDING |
| 16529      | 455           | LOW_SPENDING    |
| 15585      | 455           | LOW_SPENDING    |
| 13485      | 454           | LOW_SPENDING    |

- This dataset categorizes customers into spending cohorts based on their total spending, revealing distinct patterns in customer behavior and revenue contribution.

- **Key Observations :**

1. **High-Spending Cohort**
- **Spending Range**: $194,551 – $280,206.
- **Examples**: Customers 14646, 18102.
- These customers contribute significantly to revenue and are highly valuable.

2. **Medium-Spending Cohort**
- **Spending Range**: $745 – $746.
- **Examples**: Customers 14050, 14790.
- These customers represent a stable but moderate revenue stream.

3. **Low-Spending Cohort**
- **Spending Range**: $454 – $455.
- **Examples**: Customers 16529, 15585.
- These customers contribute minimally to revenue and may represent opportunities for re-engagement.

- **Actionable Insights :**

1. **High-Spending Cohort**
- **Action**: Reward these customers with loyalty programs, exclusive perks, or personalized offers to maintain their loyalty and encourage repeat purchases.

2. **Medium-Spending Cohort**
- **Action**: Implement upselling or cross-selling strategies to increase their spending and move them into the high-spending cohort.

3. **Low-Spending Cohort**
- **Action**: Re-engage these customers with targeted campaigns, incentives, or product recommendations to boost their spending and improve retention.

# COHORT ANALYSIS/ CUSTOMER RETENTION ANALYSIS ON CUSTOMER LEVEL :
```sql
-- COHORT ANALYSIS/ CUSTOMER RETENTION ANALYSIS ON CUSTOMER LEVEL
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
SELECT FIRST_PURCHASE_MONTH AS COHORT,
COUNT(DISTINCT IF (COHORT_MONTH = 'MONTH_0',CUSTOMERID, NULL)) AS 'MONTH_0',
COUNT(DISTINCT IF (COHORT_MONTH = 'MONTH_1',CUSTOMERID, NULL)) AS 'MONTH_1',
COUNT(DISTINCT IF (COHORT_MONTH = 'MONTH_2',CUSTOMERID, NULL)) AS 'MONTH_2',
COUNT(DISTINCT IF (COHORT_MONTH = 'MONTH_3',CUSTOMERID, NULL)) AS 'MONTH_3',
COUNT(DISTINCT IF (COHORT_MONTH = 'MONTH_4',CUSTOMERID, NULL)) AS 'MONTH_4',
COUNT(DISTINCT IF (COHORT_MONTH = 'MONTH_5',CUSTOMERID, NULL)) AS 'MONTH_5',
COUNT(DISTINCT IF (COHORT_MONTH = 'MONTH_6',CUSTOMERID, NULL)) AS 'MONTH_6',
COUNT(DISTINCT IF (COHORT_MONTH = 'MONTH_7',CUSTOMERID, NULL)) AS 'MONTH_7',
COUNT(DISTINCT IF (COHORT_MONTH = 'MONTH_8',CUSTOMERID, NULL)) AS 'MONTH_8',
COUNT(DISTINCT IF (COHORT_MONTH = 'MONTH_9',CUSTOMERID, NULL)) AS 'MONTH_9',
COUNT(DISTINCT IF (COHORT_MONTH = 'MONTH_10',CUSTOMERID, NULL)) AS 'MONTH_10',
COUNT(DISTINCT IF (COHORT_MONTH = 'MONTH_11',CUSTOMERID, NULL)) AS 'MONTH_11'
FROM CTE3
GROUP BY FIRST_PURCHASE_MONTH
ORDER BY FIRST_PURCHASE_MONTH;
```
--Output--

| COHORT     | MONTH_0 | MONTH_1 | MONTH_2 | MONTH_3 | MONTH_4 | MONTH_5 | MONTH_6 | MONTH_7 | MONTH_8 | MONTH_9 | MONTH_10 | MONTH_11 |
|------------|---------|---------|---------|---------|---------|---------|---------|---------|---------|---------|----------|----------|
| 12/1/2010  | 885     | 324     | 286     | 340     | 321     | 352     | 321     | 309     | 313     | 350     | 331      | 445      |
| 1/1/2011   | 417     | 92      | 111     | 96      | 134     | 120     | 103     | 101     | 125     | 136     | 152      | 49       |
| 2/1/2011   | 380     | 71      | 71      | 108     | 103     | 94      | 96      | 106     | 94      | 116     | 26       | 0        |
| 3/1/2011   | 452     | 68      | 114     | 90      | 101     | 76      | 121     | 104     | 126     | 39      | 0        | 0        |
| 4/1/2011   | 300     | 64      | 61      | 63      | 59      | 68      | 65      | 78      | 22      | 0       | 0        | 0        |
| 5/1/2011   | 284     | 54      | 49      | 49      | 59      | 66      | 75      | 27      | 0       | 0       | 0        | 0        |
| 6/1/2011   | 242     | 42      | 38      | 64      | 56      | 81      | 23      | 0       | 0       | 0       | 0        | 0        |
| 7/1/2011   | 188     | 34      | 39      | 42      | 51      | 21      | 0       | 0       | 0       | 0       | 0        | 0        |
| 8/1/2011   | 169     | 35      | 42      | 41      | 21      | 0       | 0       | 0       | 0       | 0       | 0        | 0        |
| 9/1/2011   | 299     | 70      | 90      | 34      | 0       | 0       | 0       | 0       | 0       | 0       | 0        | 0        |
| 10/1/2011  | 358     | 86      | 41      | 0       | 0       | 0       | 0       | 0       | 0       | 0       | 0        | 0        |
| 11/1/2011  | 324     | 36      | 0       | 0       | 0       | 0       | 0       | 0       | 0       | 0       | 0        | 0        |
| 12/1/2011  | 41      | 0       | 0       | 0       | 0       | 0       | 0       | 0       | 0       | 0       | 0        | 0        |

- This dataset provides a cohort analysis showing customer retention over 12 months for different cohorts (groups of customers based on their join month). Below is a breakdown of the key observations and actionable insights.

- **Key Observations :**

1. **December 2010 Cohort**
- **Retention**: Starts with 885 customers in Month 0 and retains 445 customers by Month 11.
- **Trend**: Steady retention with a slight increase in Month 11, possibly due to holiday season effects.

2. **January 2011 Cohort**
- **Retention**: Starts with 417 customers in Month 0 and drops to 49 customers by Month 11.
- **Trend**: Gradual decline, with a sharp drop in Month 11.

3. **Later Cohorts (February–December 2011)**
- **Retention**: Declines rapidly after Month 0, with most cohorts losing all customers by Month 6–8.
- **Trend**: Shorter retention periods, indicating weaker customer engagement over time.

- **Actionable Insights :**

1. **Leverage December 2010 Success**
- Analyze what made the December 2010 cohort successful (e.g., holiday promotions, onboarding) and replicate these strategies for other cohorts.

2. **Improve Retention for Later Cohorts**
- Investigate why retention drops sharply for cohorts like January 2011 and later. Possible reasons include:
  - Ineffective onboarding or follow-up.
  - Lack of engagement strategies post-purchase.
- Implement targeted retention campaigns, such as personalized offers or loyalty programs.

3. **Focus on Early Retention**
- Since most cohorts lose customers within 6–8 months, prioritize strategies to retain customers in the first few months (e.g., post-purchase follow-ups, discounts for repeat purchases).

# COHORT ANALYSIS ON REVENUE :
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

),
CTE3 AS (
SELECT 
CUSTOMERID,
REVENUE,
FIRST_PURCHASE_MONTH AS COHORT,
concat(
'MONTH_',
PERIOD_DIFF(
EXTRACT(YEAR_MONTH FROM PURCHASE_MONTH),
EXTRACT(YEAR_MONTH FROM FIRST_PURCHASE_MONTH)
)) AS COHORT_MONTH
FROM CTE2
)
SELECT COHORT,
ROUND(SUM(DISTINCT IF (COHORT_MONTH = 'MONTH_0',REVENUE, 0)),0) AS 'MONTH_0',
ROUND(SUM(DISTINCT IF (COHORT_MONTH = 'MONTH_1',REVENUE, 0)),0) AS 'MONTH_1',
ROUND(SUM(DISTINCT IF (COHORT_MONTH = 'MONTH_2',REVENUE, 0)),0) AS 'MONTH_2',
ROUND(SUM(DISTINCT IF (COHORT_MONTH = 'MONTH_3',REVENUE, 0)),0) AS 'MONTH_3',
ROUND(SUM(DISTINCT IF (COHORT_MONTH = 'MONTH_4',REVENUE, 0)),0) AS 'MONTH_4',
ROUND(SUM(DISTINCT IF (COHORT_MONTH = 'MONTH_5',REVENUE, 0)),0) AS 'MONTH_5',
ROUND(SUM(DISTINCT IF (COHORT_MONTH = 'MONTH_6',REVENUE, 0)),0) AS 'MONTH_6',
ROUND(SUM(DISTINCT IF (COHORT_MONTH = 'MONTH_7',REVENUE, 0)),0) AS 'MONTH_7',
ROUND(SUM(DISTINCT IF (COHORT_MONTH = 'MONTH_8',REVENUE, 0)),0) AS 'MONTH_8',
ROUND(SUM(DISTINCT IF (COHORT_MONTH = 'MONTH_9',REVENUE, 0)),0) AS 'MONTH_9',
ROUND(SUM(DISTINCT IF (COHORT_MONTH = 'MONTH_10',REVENUE, 0)),0) AS 'MONTH_10',
ROUND(SUM(DISTINCT IF (COHORT_MONTH = 'MONTH_11',REVENUE, 0)),0) AS 'MONTH_11'
FROM CTE3
GROUP BY COHORT
ORDER BY COHORT;
```
--Output--
## Cohort Revenue Table

| COHORT     | MONTH_0  | MONTH_1  | MONTH_2  | MONTH_3  | MONTH_4  | MONTH_5  | MONTH_6  | MONTH_7  | MONTH_8  | MONTH_9  | MONTH_10 | MONTH_11 |
|------------|---------|---------|---------|---------|---------|---------|---------|---------|---------|---------|----------|----------|
| 12/1/2010  | 113725  | 74541   | 59628   | 80790   | 46371   | 99181   | 89924   | 93134   | 100893  | 149378  | 145263   | 115552   |
| 1/1/2011   | 134680  | 23902   | 22425   | 43612   | 23937   | 31233   | 25140   | 26370   | 20055   | 35324   | 44199    | 13066    |
| 2/1/2011   | 26279   | 13324   | 16282   | 12573   | 9755    | 9951    | 15395   | 15743   | 16316   | 18688   | 5159     | 0        |
| 3/1/2011   | 28965   | 11354   | 17926   | 12554   | 15077   | 10097   | 16159   | 17892   | 17571   | 5807    | 0        | 0        |
| 4/1/2011   | 18711   | 9247    | 8506    | 5677    | 7220    | 7551    | 6977    | 8954    | 2611    | 0       | 0        | 0        |
| 5/1/2011   | 27070   | 7370    | 8043    | 5532    | 6904    | 7773    | 8363    | 172587  | 0       | 0       | 0        | 0        |
| 6/1/2011   | 63823   | 7317    | 5685    | 11396   | 8962    | 15845   | 4032    | 0       | 0       | 0       | 0        | 0        |
| 7/1/2011   | 15700   | 4927    | 4462    | 4734    | 5620    | 2577    | 0       | 0       | 0       | 0       | 0        | 0        |
| 8/1/2011   | 15856   | 8921    | 11286   | 17479   | 7487    | 0       | 0       | 0       | 0       | 0       | 0        | 0        |
| 9/1/2011   | 26598   | 7644    | 8833    | 3620    | 0       | 0       | 0       | 0       | 0       | 0       | 0        | 0        |
| 10/1/2011  | 31442   | 9524    | 5207    | 0       | 0       | 0       | 0       | 0       | 0       | 0       | 0        | 0        |
| 11/1/2011  | 25303   | 7298    | 0       | 0       | 0       | 0       | 0       | 0       | 0       | 0       | 0        | 0        |
| 12/1/2011  | 19637   | 0       | 0       | 0       | 0       | 0       | 0       | 0       | 0       | 0       | 0        | 0        |

- This dataset provides a cohort analysis showing revenue retention over 12 months for different cohorts (groups of customers based on their join month). Below is a breakdown of the key observations and actionable insights.

- **Key Observations :**

1. **December 2010 Cohort**
- **Revenue**: Starts with $113,725 in **Month 0** and peaks at $149,378 in **Month 9**, ending at $115,552 in **Month 11**.
- **Trend**: Strong and steady revenue retention, with a significant increase in Month 9, likely due to holiday season effects.

2. **January 2011 Cohort**
- **Revenue**: Starts with $134,680 in **Month 0** and drops to $13,066 by **Month 11**.
- **Trend**: Gradual decline, with a sharp drop in Month 11.

3. **Later Cohorts (February–December 2011)**
- **Revenue**: Declines rapidly after Month 0, with most cohorts generating $0 revenue by Month 6–8.
- **Trend**: Shorter revenue retention periods, indicating weaker customer engagement and spending over time.

4. **Outlier**
- **May 2011 Cohort**: Shows a spike in **Month 7** ($172,587), which is unusual and may indicate a data anomaly or a one-time large purchase.

- **Actionable Insights :**

1. **Leverage December 2010 Success**
- Analyze what made the December 2010 cohort successful (e.g., holiday promotions, onboarding) and replicate these strategies for other cohorts.

2. **Improve Revenue Retention for Later Cohorts**
- Investigate why revenue drops sharply for cohorts like January 2011 and later. Possible reasons include:
  - Ineffective onboarding or follow-up.
  - Lack of engagement strategies post-purchase.
- Implement targeted retention campaigns, such as personalized offers or loyalty programs.

3. **Focus on Early Revenue Retention**
- Since most cohorts lose revenue-generating customers within 6–8 months, prioritize strategies to retain customers in the first few months (e.g., post-purchase follow-ups, discounts for repeat purchases).

4. **Investigate Outliers**
- Examine the May 2011 cohort's spike in **Month 7** to determine if it was due to a specific event, promotion, or data error.

