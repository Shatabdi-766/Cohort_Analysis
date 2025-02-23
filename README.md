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
| 12/1/2010   |
| 12/2/2010   |
| 12/3/2010   |
| 12/5/2010   |
| 1/4/2011    |
| 1/5/2011    |
| 1/6/2011    |
| 1/7/2011    |
| 1/9/2011    |
| 1/10/2011   |
| 1/11/2011   |

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


