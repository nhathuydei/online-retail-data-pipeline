# Online Retail Data Pipeline & Analytics

An end-to-end data engineering and analytics project that processes retail transaction data from Excel into Parquet, loads it into MySQL, transforms it with PySpark, and builds a PostgreSQL data warehouse and data mart for Power BI reporting.

The project simulates a local data platform with separate source and analytical database layers, using Apache Airflow to orchestrate warehouse transformation and data mart refresh tasks.

## Table of Contents

- [1. Project Overview](#1-project-overview)
- [2. Architecture](#2-architecture)
- [3. Technology Stack](#3-technology-stack)
- [4. Dataset](#4-dataset)
- [5. Data Pipeline](#5-data-pipeline)
- [6. Workflow Orchestration with Airflow](#6-workflow-orchestration-with-airflow)
- [7. PostgreSQL Data Warehouse and Data Mart](#7-postgresql-data-warehouse-and-data-mart)
- [8. Power BI Dashboard](#8-power-bi-dashboard)
- [9. DAX Measures and Calculated Columns](#9-dax-measures-and-calculated-columns)
- [10. Running the Project](#10-running-the-project)
- [11. Project Structure](#11-project-structure)
- [12. Results](#12-results)
- [13. Current Scope and Limitations](#13-current-scope-and-limitations)
- [14. Potential Future Improvements](#14-potential-future-improvements)

## 1. Project Overview

This project simulates a retail data pipeline in a containerized local environment. It demonstrates data ingestion, data transformation, relational data modeling, workflow orchestration, and business intelligence reporting.

The pipeline separates the transactional source database from the analytical data warehouse:

- **MySQL** simulates the operational source database (OLTP).
- **PostgreSQL** stores the transformed warehouse data and reporting data mart (OLAP).
- **PySpark** handles data extraction, cleaning, standardization, and transformation.
- **Apache Airflow** orchestrates the warehouse transformation and data mart refresh.
- **Power BI** connects to PostgreSQL to present sales and customer analytics.

### Objectives

- Convert the original Excel dataset into Parquet format.
- Load raw retail transactions into MySQL.
- Transform and standardize source data using PySpark.
- Store transformed records in PostgreSQL.
- Build dimensional tables and aggregated sales data for analytics.
- Orchestrate downstream processing with Apache Airflow.
- Develop a Power BI dashboard for exploring sales, customers, and products.

## 2. Architecture

```text
                  Online Retail.xlsx
                         |
                         v
               Python / Pandas
               Excel to Parquet
                         |
                         v
                online_retail.parquet
                         |
                         v
              Python / MySQL Connector
                Initial / Monthly Load
                         |
                         v
               MySQL — retail_source
                   raw_sales
                  (OLTP Layer)
                         |
                         | MySQL JDBC
                         v
                   Apache Airflow
                         |
                         v
                       PySpark
              Data Cleaning & Transformation
                         |
                         | PostgreSQL JDBC
                         v
             PostgreSQL — retail_dwh
             warehouse.online_retail_clean
                   (Warehouse)
                         |
                         v
               Refresh PostgreSQL Mart
                         |
                         v
                  PostgreSQL — mart
             dim_date / dim_customer
             dim_product / fact_sales
                  agg_daily_sales
                         |
                         v
                       Power BI
                Sales Analytics Report
```

**Orchestration scope:** Excel-to-Parquet conversion and Parquet-to-MySQL ingestion are executed separately. The current Airflow DAG orchestrates two downstream tasks: MySQL-to-PostgreSQL transformation and PostgreSQL data mart refresh.

## 3. Technology Stack

| Component | Technology |
|---|---|
| Source dataset | UCI Online Retail |
| Data preparation | Python, Pandas |
| Intermediate file format | Parquet, PyArrow |
| Source database | MySQL 8.0 |
| Data transformation | PySpark 3.5.3 |
| Data warehouse and data mart | PostgreSQL 16 |
| Workflow orchestration | Apache Airflow 2.8.1 |
| Containerization | Docker, Docker Compose |
| JVM | Eclipse Temurin OpenJDK 17 |
| Business intelligence | Power BI |
| Database connectivity | MySQL JDBC, PostgreSQL JDBC, ODBC |
| Development environment | macOS on Apple Silicon |

The project is developed and tested in a local Docker environment. The current Dockerfile uses an ARM64 Temurin JDK distribution.

## 4. Dataset

This project uses the **Online Retail** dataset from the UCI Machine Learning Repository.

**Dataset source:** [UCI Machine Learning Repository — Online Retail](https://archive.ics.uci.edu/dataset/352/online%2Bretail)

The dataset contains transactions from a UK-based, non-store online retailer between December 1, 2010, and December 9, 2011. The business mainly sells unique gift products, and many customers are wholesalers.

The original dataset contains **541,909 rows**.

### Key fields

| Field | Description |
|---|---|
| `InvoiceNo` | Invoice number; invoices starting with `C` indicate cancellations |
| `StockCode` | Product identifier |
| `Description` | Product description |
| `Quantity` | Number of items purchased |
| `InvoiceDate` | Transaction date and time |
| `UnitPrice` | Price per item |
| `CustomerID` | Customer identifier |
| `Country` | Customer country |

### Dataset preparation

The original Excel file is converted into Parquet before loading into MySQL.

The expected local file paths are:

```text
data/raw/online_retail.xlsx
data/raw/online_retail.parquet
```

The raw dataset is excluded from the Git repository. Download it from the official UCI source and place it in `data/raw/` before running the preparation script.

## 5. Data Pipeline

### Step 1 — Excel to Parquet

Script: `src/convert_to_parquet.py`

The script reads the original Excel dataset and converts it into Parquet format.

Key operations:

- Reads the Excel file using Pandas.
- Uses the `python-calamine` engine for Excel reading.
- Normalizes selected text fields.
- Writes the prepared dataset to `data/raw/online_retail.parquet`.

Parquet provides a structured intermediate file format for the subsequent ingestion process.

### Step 2 — Parquet to MySQL

Script: `src/01_parquet_to_mysql.py`

The script loads records from Parquet into the MySQL source table:

```text
MySQL
└── retail_source
    └── raw_sales
```

Key implementation details:

- Reads the prepared Parquet file.
- Validates the expected source columns.
- Normalizes dates, customer IDs, text fields, and numeric values.
- Excludes records with invalid dates or numeric values.
- Inserts records into MySQL in batches.
- Supports initial and monthly loading modes.

#### Loading modes

| Mode | Purpose |
|---|---|
| `--mode initial` | Loads records before January 1, 2011 |
| `--monthly YYYY-MM` | Loads records for a specified month from January 2011 onward |

Example commands:

```bash
# Initial historical load
python src/01_parquet_to_mysql.py --mode initial

# Load January 2011
python src/01_parquet_to_mysql.py --monthly 2011-01

# Load February 2011
python src/01_parquet_to_mysql.py --monthly 2011-02
```

The source ingestion process is separate from the Airflow DAG. It can be run when initializing the source database or simulating the arrival of another month's data.

### Step 3 — MySQL to PostgreSQL using PySpark

Script: `src/02_spark_mysql_to_postgres.py`

PySpark reads raw records from MySQL through JDBC, applies data cleaning and transaction classification, and writes the transformed data to PostgreSQL.

Target table:

```text
PostgreSQL
└── retail_dwh
    └── warehouse
        └── online_retail_clean
```

#### Data cleaning and standardization

The transformation includes:

- Removes rows whose descriptions contain `test`, case-insensitively, while preserving null descriptions.
- Trims customer IDs and converts empty or invalid placeholder values to null.
- Trims descriptions.
- Normalizes stock codes to uppercase.
- Identifies canceled invoices using the `C` invoice prefix.
- Flags returns based on negative quantities.
- Flags zero or negative unit prices.
- Identifies bad-debt adjustments using the `A` invoice prefix.
- Creates customer-presence and customer-type flags.
- Classifies records into product, shipping, discount, fee, sample, manual, gift, internal, and other categories.
- Calculates `revenue` as `quantity × unitprice`.
- Removes exact duplicate rows using `dropDuplicates()`.

#### Transaction classification

The transformation creates flags such as:

| Field | Purpose |
|---|---|
| `iscanceled` | Identifies canceled invoices |
| `isreturn` | Identifies records with negative quantities |
| `isfreeorinvalidprice` | Identifies zero or negative unit prices |
| `isbaddebtadjustment` | Identifies bad-debt adjustment invoices |
| `hascustomer` | Indicates whether a customer ID is available |
| `customertype` | Classifies customer records |
| `isvalidtransaction` | Indicates whether a record meets the defined transaction validity conditions |
| `producttype` | Classifies the type of transaction item |
| `revenue` | Quantity multiplied by unit price |

`isvalidtransaction` is retained as a classification field. It is not used as a general filter to remove every record considered invalid from the warehouse.

**Loading behavior:** The current PySpark process overwrites the target warehouse table when it runs. It is a full-refresh process rather than an incremental merge.

## 6. Workflow Orchestration with Airflow

DAG file: `dags/retail_etl_dag.py`

DAG ID: `retail_production_pipeline`

The current DAG contains two tasks:

1. `spark_mysql_to_postgres`
2. `build_postgres_data_mart`

Execution sequence:

```text
spark_mysql_to_postgres
           |
           v
build_postgres_data_mart
```

### DAG configuration

- `schedule=None` — manual execution only.
- `catchup=False`.
- One retry for a failed task.
- A two-minute retry delay.

The DAG is triggered manually through the Airflow UI. Automatic scheduled execution is not part of the current verified workflow.

### Task 1 — MySQL to PostgreSQL

Runs the PySpark transformation script to read source records from MySQL and write the transformed warehouse table to PostgreSQL.

### Task 2 — Refresh PostgreSQL data mart

Runs:

```text
sql/transform/refresh_postgres_mart.sql
```

This SQL script refreshes the reporting tables in the `mart` schema.

### Important behavior

The current workflow rebuilds downstream data rather than incrementally merging new records:

- The PySpark process overwrites `warehouse.online_retail_clean`.
- The mart refresh SQL truncates and repopulates the reporting tables.

Consequently, the current workflow is suitable for demonstrating a full-refresh pipeline, but it is not yet an incremental production pipeline.

## 7. PostgreSQL Data Warehouse and Data Mart

The PostgreSQL database separates transformed warehouse data from reporting-oriented tables.

### Warehouse layer

```text
retail_dwh
└── warehouse
    └── online_retail_clean
```

This layer stores the standardized and classified transaction records produced by PySpark.

### Data mart layer

```text
retail_dwh
└── mart
    ├── dim_date
    ├── dim_customer
    ├── dim_product
    ├── fact_sales
    └── agg_daily_sales
```

### Data mart model

```text
dim_date ─────────┐
                  |
dim_customer ─────┼──> fact_sales
                  |
dim_product ──────┘

fact_sales ───────────> agg_daily_sales
```

| Table | Purpose |
|---|---|
| `mart.dim_date` | Date attributes for time-based analysis |
| `mart.dim_customer` | Customer identifiers, customer types, and purchase date ranges |
| `mart.dim_product` | Product codes, descriptions, and product types |
| `mart.fact_sales` | Product-level sales records used for analytics |
| `mart.agg_daily_sales` | Daily revenue and quantity aggregation |

### Fact table filtering rules

The `fact_sales` table is built from records classified as `Product`.

The data mart excludes:

- Canceled invoices.
- Bad-debt adjustments.
- Records classified as `Manual`.
- Records classified as `Sample`.
- Records classified as `Internal`.

**Returns are retained** when they meet the other filtering conditions. Since revenue is calculated as quantity multiplied by unit price, negative quantities can contribute negative revenue.

The resulting revenue reflects the impact of retained returns. It should not automatically be interpreted as an accounting-certified net revenue figure.

The current mart process does not use `isvalidtransaction` as a general filter when creating `fact_sales`.

## 8. Power BI Dashboard

File: `bi/Retail_Sales_Performance.pbix`

Power BI connects to the local PostgreSQL instance through ODBC, using the `retail_dwh` database and the `mart` schema.

The semantic model follows a star-schema structure, with `fact_sales` connected to:

- `dim_date`
- `dim_customer`
- `dim_product`

The provided DAX measures are organized in a dedicated `_Measures` table.

### Page 1 — Sales Performance & Trends

Focuses on overall sales performance and time-based trends.

Key metrics and analysis include:

- Total Revenue
- Total Orders
- Average Order Value
- Active Customers
- Revenue and order trends over time
- Revenue analysis by day of the week
- Date, country, and product-related slicers

The report highlights weekday sales patterns, including Thursday as the highest-revenue day and Saturday as a day with no orders in the observed report context. These observations should be interpreted within the dataset and report filters.

### Page 2 — Customer & Product Deep-Dive

Explores customer and product performance.

Key analysis includes:

- Top products by revenue and quantity.
- Top customers.
- Customer scatter plot comparing order count and revenue.
- Monthly analysis of new and repeat customers.

The report is intended for exploratory business analysis rather than establishing causal explanations for sales performance.

## 9. DAX Measures and Calculated Columns

The following expressions document the measures and calculated columns supplied for this project.

### Measures

#### Total Revenue

```dax
Total Revenue = SUM(fact_sales[revenue])
```

#### Total Orders

```dax
Total Orders = DISTINCTCOUNT(fact_sales[invoiceno])
```

#### Total Quantity

```dax
Total Quantity = SUM(fact_sales[quantity])
```

#### Avg Order Value

```dax
Avg Order Value =
DIVIDE(
    [Total Revenue],
    [Total Orders],
    0
)
```

#### Active Customers

```dax
Active Customers =
CALCULATE(
    DISTINCTCOUNT(fact_sales[customerid]),
    fact_sales[customerid] <> "UNKNOWN"
)
```

#### Repeat Customer %

```dax
Repeat Customer % =
VAR _known_customers =
    FILTER(
        VALUES(fact_sales[customerid]),
        fact_sales[customerid] <> "UNKNOWN"
    )
VAR _repeat_customers =
    CALCULATE(
        COUNTROWS(
            FILTER(
                _known_customers,
                [Total Orders] > 1
            )
        )
    )
RETURN
    DIVIDE(
        _repeat_customers,
        [Active Customers],
        0
    )
```

This measure identifies customers with more than one distinct invoice in the current filter context. Its result can change depending on the selected date range and other report filters.

#### Revenue PM

```dax
Revenue PM =
CALCULATE(
    [Total Revenue],
    DATEADD(dim_date[date_key], -1, MONTH)
)
```

#### Revenue MoM Growth %

```dax
Revenue MoM Growth % =
VAR _diff = [Total Revenue] - [Revenue PM]
RETURN
    DIVIDE(_diff, [Revenue PM], 0)
```

#### Orders per Customer

```dax
Orders per Customer =
DIVIDE(
    [Total Orders],
    [Active Customers],
    0
)
```

#### Average Selling Price

```dax
Average Selling Price =
DIVIDE(
    [Total Revenue],
    [Total Quantity],
    0
)
```

#### Revenue MoM Label

```dax
Revenue MoM Label =
VAR _current = [Total Revenue]
VAR _previous = [Revenue PM]
VAR _growth_pct = [Revenue MoM Growth %]
VAR _icon =
    SWITCH(
        TRUE(),
        ISBLANK(_previous) || ISBLANK(_current), BLANK(),
        _growth_pct > 0, UNICHAR(11165),
        _growth_pct < 0, UNICHAR(11167),
        "-"
    )
VAR _formatted_pct =
    SWITCH(
        TRUE(),
        ISBLANK(_growth_pct), "N/A",
        _growth_pct > 0, "+" & FORMAT(_growth_pct, "0.0%"),
        FORMAT(_growth_pct, "0.0%")
    )
RETURN
    IF(
        ISBLANK(_previous),
        "No prior period data",
        _icon & " " & _formatted_pct & " vs PM"
    )
```

#### Revenue MoM Color

```dax
Revenue MoM Color =
VAR _growth_pct = [Revenue MoM Growth %]
RETURN
    SWITCH(
        TRUE(),
        ISBLANK(_growth_pct), "#7F8C8D",
        _growth_pct > 0, "#107C41",
        _growth_pct < 0, "#C50F1F",
        "#595959"
    )
```

### Calculated columns

#### Market Segment

```dax
Market Segment =
IF(
    fact_sales[is_domestic] = "1",
    "United Kingdom",
    "International"
)
```

#### Day Type

```dax
Day Type =
IF(
    dim_date[is_weekend] = "1",
    "Weekend",
    "Weekday"
)
```

**Validation note:** The PostgreSQL SQL definitions use Boolean fields for `is_domestic` and `is_weekend`. If Power BI imports these columns as Boolean, the calculated column expressions should use Boolean comparisons rather than the text value `"1"`.

## 10. Running the Project

### Prerequisites

Prepare the following:

- Docker Desktop with Docker Compose.
- Python environment for running local preparation scripts.
- Power BI Desktop to open the dashboard.
- An ODBC driver compatible with PostgreSQL.

The current Dockerfile downloads an ARM64 Eclipse Temurin JDK 17 distribution. The setup therefore targets an ARM64 container environment.

### Step 1 — Prepare environment variables

Copy the example environment file:

```bash
cp .env.example .env
```

Review the variables and update credentials as needed.

Do not commit `.env` or real credentials to the repository.

The Compose configuration uses Docker service names for connections between containers. Desktop tools such as Power BI connect through the host-facing published ports, usually using `localhost`.

### Step 2 — Prepare the dataset

Download the Online Retail Excel dataset from the official UCI source and place it at:

```text
data/raw/online_retail.xlsx
```

Install the local Python dependencies if needed, then run:

```bash
python src/convert_to_parquet.py
```

The output should be:

```text
data/raw/online_retail.parquet
```

The raw Excel and Parquet files are intentionally excluded from the Git repository.

### Step 3 — Build and start Docker services

From the project root:

```bash
docker compose build
docker compose up -d
docker compose ps
```

Wait for the database and Airflow services to finish initializing before proceeding.

### Step 4 — Load data into MySQL

Run the initial historical load:

```bash
docker exec -it retail_airflow_webserver \
    python /opt/airflow/src/01_parquet_to_mysql.py --mode initial
```

For a monthly load, use:

```bash
docker exec -it retail_airflow_webserver \
    python /opt/airflow/src/01_parquet_to_mysql.py --monthly 2011-01
```

The monthly mode can be used to simulate data arriving month by month.

### Step 5 — Trigger the Airflow DAG

Open Airflow:

```text
http://localhost:8080
```

Use the credentials configured in the local Compose environment.

In the Airflow UI:

1. Find `retail_production_pipeline`.
2. Trigger the DAG manually.
3. Monitor `spark_mysql_to_postgres`.
4. Monitor `build_postgres_data_mart`.
5. Confirm that both tasks finish successfully.

The current DAG uses `schedule=None`, so it does not run automatically on a cron schedule.

### Step 6 — Open the Power BI dashboard

Open:

```text
bi/Retail_Sales_Performance.pbix
```

Configure the PostgreSQL ODBC connection:

| Setting | Value |
|---|---|
| Host | `localhost` |
| Port | `5432` |
| Database | `retail_dwh` |
| Schema | `mart` |

Use the credentials configured in the local environment. If the published host port or database credentials have changed, update the connection settings accordingly.

### Step 7 — Stop the environment

To stop running containers while retaining database volumes:

```bash
docker compose stop
```

To remove containers and the network:

```bash
docker compose down
```

Named volumes are retained by default. Avoid `docker compose down -v` unless you intentionally want to delete persistent database data and rebuild the environment from scratch.

## 11. Project Structure

```text
.
├── dags/
│   └── retail_etl_dag.py
│
├── src/
│   ├── convert_to_parquet.py
│   ├── 01_parquet_to_mysql.py
│   └── 02_spark_mysql_to_postgres.py
│
├── sql/
│   ├── migrations/
│   │   ├── V1__create_warehouse_schema.sql
│   │   └── V2__create_mart_schema.sql
│   │
│   ├── setup/
│   │   ├── airflow_metadata_setup.sql
│   │   └── mysql_source_init.sql
│   │
│   └── transform/
│       └── refresh_postgres_mart.sql
│
├── data/
│   └── raw/
│       ├── online_retail.xlsx       # Download locally
│       └── online_retail.parquet   # Generated locally
│
├── bi/
│   └── Online_Retail_v2.pbix
│
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

## 12. Results

The project demonstrates the following end-to-end workflow:

1. Prepare the original Excel dataset as Parquet.
2. Load source records into MySQL.
3. Extract and transform records using PySpark.
4. Store standardized records in PostgreSQL.
5. Refresh dimensional and aggregated reporting tables.
6. Orchestrate downstream processing through Airflow.
7. Connect Power BI to the PostgreSQL data mart for analysis.

The pipeline is designed around the original 541,909-row Online Retail dataset.

Row counts and analytical results should be checked against the actual database after each successful run. Since the current process uses full refreshes, the resulting counts may change when the source data or transformation rules change.

## 13. Current Scope and Limitations

- The project runs in a local Docker development environment.
- The Excel-to-Parquet and Parquet-to-MySQL steps are executed separately from the Airflow DAG.
- The Airflow DAG currently contains two downstream tasks and is manually triggered.
- Automatic scheduled execution has not been verified.
- The PySpark warehouse load overwrites the target table.
- The data mart refresh truncates and repopulates reporting tables.
- The current pipeline does not implement incremental warehouse or mart loading.
- A comprehensive automated data-quality test suite has not yet been implemented.
- Returns are retained in the product fact table when they meet the other filtering conditions, so negative quantities can contribute negative revenue.
- Power BI calculated columns should be checked against the Boolean data types of the source fields.
- This is a personal learning and portfolio project, not a production deployment.

## 14. Potential Future Improvements

Possible enhancements for future versions include:

- Integrate source ingestion into the Airflow workflow.
- Implement incremental loading instead of full-table overwrites.
- Configure and verify scheduled DAG execution.
- Simulate daily or monthly data arrival more systematically.
- Add automated data-quality checks and validation queries.
- Improve pipeline logging and failure diagnostics.
- Add test cases for cancellations, returns, missing customer IDs, and invalid prices.
- Validate Power BI calculations and filter-context behavior.
- Improve dashboard layout and support month-level reporting.

---

**Project focus:** Data ingestion, Parquet-based data preparation, PySpark ETL, PostgreSQL data warehousing, Airflow orchestration, dimensional modeling, and Power BI analytics.