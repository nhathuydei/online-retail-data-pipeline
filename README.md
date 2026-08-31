Online Retail Data Pipeline & Analytics

📌 Overview

This project builds an end-to-end data pipeline for an online retail business, starting from raw transactional data and ending with an analytics dashboard.

The project simulates a business scenario where an online retailer generates transaction data during its daily operations. The data is extracted from a raw data source, processed and cleaned using PySpark, stored in a PostgreSQL data warehouse, and then transformed into an analytics-oriented data mart for Power BI.

The main goal of the project is to practice how raw transactional data can be transformed into structured data that supports business analysis and decision-making.

The dashboard focuses on questions such as:

* How much revenue is generated?
* How many orders and customers are there?
* How does revenue change over time?
* Which products generate the most revenue?
* How many customers are returning customers?
* How many new customers are acquired each month?
* How is revenue distributed across days of the week?
* Which customers contribute the most revenue?

A machine learning branch for future sales/customer prediction is also planned, but is not part of the current V1 scope.

⸻

🏢 Business Context

The dataset represents transactions from a UK-based non-store online retailer selling unique gift products.

The business primarily sells through online channels, with many customers being wholesalers.

For this project, the dataset is treated as transactional data generated from the company’s daily sales operations.

The project uses the transaction data to simulate a simplified data engineering workflow:

Operational Transaction Data
          ↓
      Raw Storage
          ↓
       ETL Process
          ↓
      Data Warehouse
          ↓
       Data Mart
          ↓
      Power BI
          ↓
 Business Analysis & Monitoring

The project does not attempt to reproduce the exact production architecture of the original retailer. Instead, it uses a simplified architecture to demonstrate core data engineering concepts.

⸻

📊 Dataset

Dataset:

Online Retail Dataset

The dataset contains transactions occurring between:

* 01/12/2010
* 09/12/2011

The original dataset contains the following columns:

Column	Description
InvoiceNo	Invoice / transaction identifier
StockCode	Product identifier
Description	Product description
Quantity	Quantity of items purchased
InvoiceDate	Transaction timestamp
UnitPrice	Price per item
CustomerID	Customer identifier
Country	Customer country

Source file used in the project:

Online Retail.xlsx

⸻

🏗️ Architecture

The current V1 pipeline consists of four main stages.

Online Retail.xlsx
       │
       ▼
     MySQL
   raw_sales
       │
       │ Extract
       ▼
    PySpark
  Clean / Transform
       │
       ▼
 PostgreSQL
   Warehouse
       │
       │ Transform / Model
       ▼
      Mart
       │
       ▼
    Power BI

Technologies

Technology	Purpose
Python	Data ingestion and pipeline implementation
MySQL	Raw data storage
PySpark	ETL and data transformation
PostgreSQL	Data warehouse and analytical storage
Power BI	Dashboard and data visualization
ODBC	Power BI → PostgreSQL connectivity

The project currently runs locally on a MacBook.

Power BI is run inside a virtual machine on the same machine and connects to PostgreSQL through ODBC.

⸻

🔄 Data Pipeline

1. Raw Data Ingestion

The raw dataset is provided as an Excel file:

Online Retail.xlsx

Python is used to load the raw data into MySQL.

The raw table is:

MySQL
└── raw_data
    └── raw_sales

The raw layer is intentionally kept close to the original source structure so that the original transaction data can be retained before transformation.

⸻

2. Extract

PySpark connects to MySQL through JDBC and reads the raw_sales table.

MySQL raw_sales
       ↓
     PySpark

PySpark is used as the main ETL processing engine.

⸻

3. Transform

Several data quality and business rules are applied in PySpark.

Data type and text standardization

* Cast CustomerID to string
* Trim StockCode
* Convert StockCode to uppercase
* Trim Description

Test records

Records containing "test" in the description are excluded.

df.filter(~F.col("Description").ilike("%test%"))

Transaction classification

The pipeline creates several flags:

IsCanceled
IsReturn
IsFreeOrInvalidPrice
IsBadDebtAdjustment
HasCustomer
CustomerType
IsValidTransaction

Cancellation

Invoices beginning with C are classified as cancelled transactions.

InvoiceNo starts with C
        ↓
IsCanceled = true

Bad debt adjustment

Invoices beginning with A are treated as accounting adjustments related to bad debt.

InvoiceNo starts with A
        ↓
IsBadDebtAdjustment = true

These records are retained in the warehouse with their business classification rather than being immediately deleted.

Returns

Negative quantities are identified using:

Quantity < 0
        ↓
IsReturn = true

Invalid / free-price records

Records with a non-positive unit price are identified using:

UnitPrice <= 0
        ↓
IsFreeOrInvalidPrice = true

Valid transactions

A record is considered a valid transaction when:

* Quantity is not null
* UnitPrice is not null
* Quantity != 0
* UnitPrice > 0
* the invoice is not cancelled
* the invoice is not a bad debt adjustment

⸻

4. Product Classification

The pipeline derives a ProductType column based on StockCode.

Current business classification:

ProductType	Rule
Shipping	POST, C2
Discount	DOT
Fee	AMAZONFEE, BANK CHARGES
Sample	S
Manual	M
Gift	StockCode starts with GIFT
Internal	StockCode starts with DCGS
Product	Other stock codes

Revenue is calculated as:

Revenue = Quantity × UnitPrice

⸻

5. Duplicate Handling

The pipeline removes exact duplicate rows using:

df = df.dropDuplicates()

The project does not currently treat:

InvoiceNo + StockCode + InvoiceDate

as a unique key.

During data investigation, multiple records were found with the same three fields but different quantities.

For example, the dataset contains records where:

InvoiceNo + StockCode + InvoiceDate

are identical while Quantity differs.

Therefore, deduplicating using only these three fields could incorrectly remove valid transaction data.

The current V1 approach only removes full-row duplicates.

⸻

🗄️ Data Warehouse

PostgreSQL is used as the warehouse layer.

Current database:

retail_dwh
└── warehouse
    └── online_retail_clean

The warehouse table stores the cleaned and enriched transaction-level data produced by PySpark.

Current schema includes:

invoiceno
stockcode
description
quantity
invoicedate
unitprice
customerid
country
iscanceled
isreturn
isfreeorinvalidprice
isbaddebtadjustment
hascustomer
customertype
isvalidtransaction
producttype
revenue

The warehouse layer is intended to retain a relatively detailed version of the cleaned transaction data.

This allows the data to be reused for different downstream purposes instead of designing the warehouse exclusively around the Power BI dashboard.

⸻

📦 Data Mart

The data mart is the analytical layer consumed by Power BI.

The current design separates the detailed warehouse layer from the presentation-oriented mart layer.

Conceptually:

WAREHOUSE
    │
    │ Transform / filter / model
    ▼
MART
    │
    ▼
Power BI

The mart contains analytical structures such as:

mart
├── dim_date
├── dim_product
├── dim_customer
├── fact_sales
└── agg_daily_sales

Dimension tables

dim_product

Contains product-level information such as:

* StockCode
* Description
* ProductType
* ProductTypeGroup

Product groups are classified into broader analytical categories:

Core
Operational
Adjustment
Other

dim_customer

Contains customer-level information such as:

* Customer ID
* Customer type
* First purchase date
* Last purchase date

dim_date

Provides date attributes for time-based analysis, including:

* Year
* Month
* Year-Month
* Day
* Day of week
* Quarter
* Weekend indicator

Fact table

fact_sales contains transaction-level sales information used by Power BI for analysis.

⸻

📈 Power BI Dashboard

Power BI connects to the PostgreSQL mart layer.

The dashboard is designed to provide a high-level view of business performance that could be used by business or operations stakeholders to monitor sales activity and investigate areas for improvement.

Current metrics and analysis include:

Sales Performance

* Total Revenue
* Total Orders
* Average Order Value
* Revenue by Month
* Orders by Day of Week
* Revenue by Day of Week
* Revenue by Country
* Revenue by Year / Month

Customer Analysis

* Total Customers
* Active Customers
* New Customers by Month
* Repeat Customers
* Repeat Customer %
* Top Customers by Revenue
* Customer Count by Customer Type
* Revenue by Customer Type

Product Analysis

* Total SKUs
* Product Revenue
* Product Quantity
* Revenue by Product
* Top Products by Revenue
* Revenue by Product Type / Product Type Group

⸻

🧮 Example DAX Measures

Some of the current Power BI measures include:

Active Customers

Active Customers =
CALCULATE(
    DISTINCTCOUNT(fact_sales[customerid]),
    fact_sales[customerid] <> "UNKNOWN"
)

Average Order Value

Avg Order Value =
DIVIDE(
    [Product Revenue],
    DISTINCTCOUNT(fact_sales[invoiceno])
)

Repeat Customer %

Repeat Customer % =
VAR _repeat =
    CALCULATE(
        DISTINCTCOUNT(fact_sales[customerid]),
        FILTER(
            VALUES(fact_sales[customerid]),
            CALCULATE(
                DISTINCTCOUNT(fact_sales[invoiceno])
            ) > 1
        ),
        fact_sales[customerid] <> "UNKNOWN"
    )
RETURN
    DIVIDE(
        _repeat,
        [Active Customers]
    )

Product Revenue

Product Revenue =
CALCULATE(
    SUM(fact_sales[revenue]),
    dim_product[producttypegroup] = "Core"
)

Operational Revenue

Operational Revenue =
CALCULATE(
    SUM(fact_sales[revenue]),
    dim_product[producttypegroup] = "Operational"
)

⸻

🔎 Business Insights Explored

The dashboard is intended not only to display numbers but also to support business observation.

Examples of observations explored during development include:

* Revenue distribution across months
* Revenue concentration among customers
* New customer acquisition over time
* Repeat customer behavior
* Highest-revenue products
* Revenue distribution by day of week
* Thursday showing the highest observed revenue among weekdays
* Saturday showing no observed sales activity in the analyzed data
* Sunday having sales activity

These observations demonstrate how transaction data can be transformed into information that can be investigated from an operational/business perspective.

⸻

📁 Project Structure

Current project structure:

.
├── data pipeline/
│   ├── dataset/
│   │   ├── Online Retail.xlsx
│   │   └── online+retail.zip
│   │
│   ├── load_csv_to_mysql.py
│   ├── dm_load_csv_to_mysql.ipynb
│   ├── spark_transform.py
│   ├── 01_ddl_schema.sql
│   ├── datacheck.ipynb
│   ├── workflow.txt
│   │
│   ├── 1dm_spark_transform.ipynb
│   ├── 2dm_spark_transform.ipynb
│   └── dm_spark_transform.ipynb
│
├── power Bi/
│   └── lab0/
│       ├── sale perfomance dashboard.pbix
│       └── ...
│
└── ...

Some notebooks and SQL scripts are experimental/development versions from different stages of the project.

The main intended pipeline is:

load raw data
      ↓
MySQL raw_sales
      ↓
PySpark transformation
      ↓
PostgreSQL warehouse
      ↓
PostgreSQL mart
      ↓
Power BI

⸻

🚀 Current Status

V1 — Current Scope

The primary goal of V1 is to demonstrate an end-to-end data engineering workflow.

Completed / demonstrated:

* Raw dataset ingestion
* MySQL raw data storage
* PySpark JDBC extraction
* Data cleaning and transformation
* Transaction classification
* Product classification
* Revenue calculation
* PostgreSQL warehouse design
* Data mart design
* Dimension and fact modeling
* Power BI connection to PostgreSQL
* DAX measures
* Business-oriented dashboard analysis

The Power BI dashboard is currently functional but still at an early stage and is not considered production-quality.

⸻

🔮 Future Improvements

1. Orchestration with Apache Airflow

The current pipeline is manually executed.

A future version will introduce Apache Airflow to orchestrate the workflow.

Target flow:

Airflow DAG
    │
    ├── Load raw data
    │
    ├── Run PySpark ETL
    │
    ├── Build / refresh warehouse
    │
    ├── Build / refresh mart
    │
    └── Refresh downstream analytics

The DAG can eventually be scheduled to run automatically instead of requiring manual execution.

⸻

2. Machine Learning

A future branch of the project may use the warehouse data to create a training dataset for a simple machine learning model.

Potential use cases include:

* Sales forecasting
* Customer behavior prediction
* Customer segmentation
* Product demand prediction

The ML component is intentionally outside the scope of the current V1.

The priority is first to complete and stabilize the end-to-end data pipeline.

⸻

3. Pipeline Improvements

Potential future improvements include:

* Automated orchestration
* Incremental loading
* Better data quality checks
* More robust error handling
* Production-style configuration management
* Improved dashboard design
* Automated testing
* Better warehouse and mart refresh strategy
* ML training pipeline

⸻

🎯 Project Objective

The main objective of this project is not to reproduce a production retail data platform.

Instead, it is a hands-on implementation of a simplified data engineering workflow:

Raw Data
   ↓
Ingestion
   ↓
ETL
   ↓
Data Warehouse
   ↓
Data Mart
   ↓
BI Dashboard
   ↓
Business Analysis

The project demonstrates practical experience with:

* Python
* SQL
* PySpark
* ETL concepts
* Data warehouse concepts
* Dimensional modeling
* PostgreSQL
* MySQL
* Power BI
* DAX
* JDBC-based data movement
* Business-oriented data analysis