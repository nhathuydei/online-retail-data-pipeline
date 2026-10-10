# Online Retail Data Pipeline & Analytics

An end-to-end data engineering and analytics project that processes retail transaction data from Excel into MySQL, transforms it with PySpark, loads it into a PostgreSQL data warehouse and data mart, and visualizes business metrics in Power BI.

## 1. Project Overview

This project simulates a retail data pipeline using a containerized local environment. It demonstrates data ingestion, transformation, relational data modeling, workflow orchestration, and business intelligence reporting.

The pipeline uses **Apache Airflow** to orchestrate the transformation and data mart creation steps. The initial Excel-to-MySQL ingestion is executed separately when the source data needs to be loaded or reloaded.

### Objectives

- Ingest raw retail transactions from Excel into MySQL.
- Transform and standardize transaction data with PySpark.
- Store transformed data in PostgreSQL as a warehouse layer.
- Build a dimensional data mart for sales analytics.
- Orchestrate warehouse and data mart processing with Apache Airflow.
- Build a Power BI dashboard to explore sales trends, customers, and products.

## 2. Architecture

```text
Online Retail.xlsx
       |
       v
Python / Pandas
       |
       v
MySQL — retail_source
Raw transactional data
       |
       | MySQL JDBC
       v
PySpark
Data cleaning and transformation
       |
       | PostgreSQL JDBC
       v
PostgreSQL — retail_dwh
warehouse.online_retail_clean
       |
       | Apache Airflow orchestrates:
       | 1. Initialize warehouse schema
       | 2. Run PySpark transformation
       | 3. Build data mart
       v
PostgreSQL — mart schema
       |
       v
Power BI via ODBC
Sales Performance & Customer Analytics
```

**Orchestration note:** The Excel-to-MySQL ingestion script is run separately. The Airflow DAG orchestrates the warehouse initialization, PySpark transformation, and data mart creation steps.

## 3. Technology Stack

| Component | Technology |
|---|---|
| Source dataset | Online Retail dataset |
| Source ingestion | Python, Pandas, MySQL Connector |
| Source database | MySQL 8.0 |
| Data transformation | PySpark 4.0.4 |
| Data warehouse and data mart | PostgreSQL 16 |
| Workflow orchestration | Apache Airflow 2.8.1 |
| Containerization | Docker, Docker Compose |
| JVM | Eclipse Temurin OpenJDK 17 |
| Business intelligence | Power BI |
| Database connectivity | MySQL JDBC, PostgreSQL JDBC, ODBC |
| Development environment | macOS on Apple Silicon |

The project was developed and tested in a local environment. The exact versions above reflect the environment used during development; they are not a guarantee that every other environment will behave identically.

## 4. Dataset

This project uses the **Online Retail** dataset from the UCI Machine Learning Repository.

**Dataset source:** [UCI Machine Learning Repository — Online Retail](https://archive.ics.uci.edu/dataset/352/online%2Bretail?utm_source=chatgpt.com)

The dataset contains transactions from a UK-based, non-store online retailer between December 1, 2010, and December 9, 2011. The business mainly sells gifts, and many customers are wholesalers.

The original dataset contains **541,909 rows**.

Key fields include:

- `InvoiceNo`
- `StockCode`
- `Description`
- `Quantity`
- `InvoiceDate`
- `UnitPrice`
- `CustomerID`
- `Country`

### Download and placement

The Excel dataset is not included in this repository. Download it from the official source above and place it at:

```text
data/raw/online_retail.xlsx
```

Make sure the filename and path match the ingestion script's configuration.

## 5. Data Pipeline

### Step 1 — Excel to MySQL

Script: `src/01_excel_to_mysql.py`

The script reads the Excel dataset with Python and Pandas and loads the records into the MySQL source table, `raw_sales`.

Key implementation details:

- Reads the Excel source file.
- Loads records in batches of 20,000 rows.
- Inserts records into MySQL and commits each batch.
- Truncates the target source table before reloading the dataset.

**Important:** Running this script reloads the source table. Do not run it merely to refresh the downstream warehouse unless you intend to replace the current source data.

### Step 2 — MySQL to PostgreSQL using PySpark

Script: `src/02_spark_mysql_to_postgres.py`

PySpark reads raw records from MySQL through JDBC, applies standardization and transaction classification, and writes the transformed records to PostgreSQL.

Transformations include:

- Removing rows whose descriptions contain `test`, case-insensitively.
- Trimming customer IDs and converting empty or invalid placeholder values to null.
- Trimming descriptions and normalizing stock codes to uppercase.
- Identifying canceled invoices using the `C` invoice prefix.
- Flagging returns based on negative quantities.
- Flagging zero or negative unit prices.
- Identifying bad-debt adjustments using the `A` invoice prefix.
- Creating customer-presence and customer-type flags.
- Classifying records into product, shipping, discount, fee, sample, manual, gift, and internal categories.
- Calculating `revenue` as `quantity × unitprice`.
- Removing exact duplicate rows using `dropDuplicates()`.

The transformed data is written to:

```text
PostgreSQL
└── warehouse
    └── online_retail_clean
```

The `isvalidtransaction` field is a classification flag. It is not used as a general filter that removes all invalid transactions from the transformed table or data mart.

### Step 3 — PostgreSQL warehouse and data mart

SQL files:

- `sql/01_init_postgres_dwh.sql`
- `sql/02_build_postgres_mart.sql`

The warehouse initialization script creates the `warehouse.online_retail_clean` table and its constraints and indexes.

The data mart script recreates the `mart` schema and builds the reporting tables.

#### Data mart model

```text
dim_date ─────────┐
                  |
dim_customer ─────┼──> fact_sales
                  |
dim_product ──────┘

fact_sales ───────────> agg_daily_sales
```

The model includes:

| Table | Purpose |
|---|---|
| `mart.dim_date` | Date attributes for time-based analysis |
| `mart.dim_customer` | Customer identifiers, customer types, and purchase date ranges |
| `mart.dim_product` | Product codes, descriptions, and product types |
| `mart.fact_sales` | Product-level sales records used for analytics |
| `mart.agg_daily_sales` | Daily revenue and quantity aggregation |

The `fact_sales` table is built from records classified as `Product`. The data mart excludes canceled invoices, bad-debt adjustments, and records classified as `Manual`, `Sample`, or `Internal` from its base view.

**Returns are retained in the product fact table when they meet the other filtering conditions.** Because revenue is calculated as quantity multiplied by unit price, negative quantities can contribute negative revenue. The resulting revenue therefore reflects the impact of retained returns; it should not automatically be interpreted as an accounting-certified net revenue figure.

The current model does not use `isvalidtransaction` as a filter when creating `fact_sales`.

## 6. Workflow Orchestration with Airflow

DAG file: `dags/retail_etl_dag.py`

DAG ID: `retail_production_pipeline`

The DAG contains three tasks:

1. `init_postgres_dwh_schema`
2. `spark_mysql_to_postgres`
3. `build_postgres_data_mart`

The tasks run in sequence:

```text
init_postgres_dwh_schema
          |
          v
spark_mysql_to_postgres
          |
          v
build_postgres_data_mart
```

The DAG is configured with:

- `catchup=False`
- One retry for a failed task
- A two-minute retry delay
- Cron expression `0 2 * * *`

The cron expression is configured in the DAG, but automatic scheduled execution and its effective timezone have not been verified. The documented successful run was triggered manually through Airflow.

### Important behavior

The warehouse initialization SQL drops and recreates `warehouse.online_retail_clean`. The data mart SQL also drops and recreates the `mart` schema.

As a result, each successful DAG run rebuilds these downstream tables rather than incrementally merging new records into the existing tables.

## 7. Power BI Dashboard

File: `bi/Retail_Sales_Performance.pbix`

Power BI connects to the local PostgreSQL instance through ODBC, using the `retail_dwh` database and the `mart` schema.

The semantic model uses a star-schema structure with `fact_sales` connected to `dim_date`, `dim_customer`, and `dim_product`.

The provided DAX measures are organized in a dedicated `_Measures` table.

### Page 1 — Sales Performance & Trends

The first page focuses on overall sales performance and time-based trends.

It includes:

- Total Revenue
- Total Orders
- Average Order Value
- Active Customers
- Revenue and order trends over time
- Revenue analysis by day of the week
- Date, country, and product-related slicers

The current dashboard analysis highlights weekday sales patterns, including Thursday as the highest-revenue day and Saturday as a day with no orders in the current report context. These observations should be interpreted within the dataset and report filters.

### Page 2 — Customer & Product Deep-Dive

The second page explores customer and product performance.

It includes:

- Top products by revenue and quantity
- Top customers
- A customer scatter plot comparing order count and revenue
- Monthly analysis of new and repeat customers

The report is intended to support exploratory business analysis rather than to establish causal explanations for sales performance.

## 8. DAX Measures and Calculated Columns

The following list includes the DAX expressions supplied for this project. No additional measure, including `Operational Revenue`, is included.

### Measures

#### Total Revenue

```dax
Total Revenue =
SUM(fact_sales[revenue])
```

#### Total Orders

```dax
Total Orders =
DISTINCTCOUNT(fact_sales[invoiceno])
```

#### Total Quantity

```dax
Total Quantity =
SUM(fact_sales[quantity])
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

This measure identifies customers with more than one distinct invoice in the current filter context. Its result can therefore change depending on the selected date range and other report filters.

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
IF(fact_sales[is_domestic] = "1", "United Kingdom", "International")
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

**Validation note:** The supplied PostgreSQL SQL creates `is_domestic` and `is_weekend` as Boolean columns. The two calculated columns above compare those fields to the text value `"1"`. Verify the data types and expressions in Power BI; if the columns are imported as Boolean, the comparisons should be adjusted to Boolean comparisons.

## 9. Running the Project

### Prerequisites

Install or prepare:

- Docker Desktop with Docker Compose
- Power BI Desktop to open the dashboard
- An ODBC driver compatible with PostgreSQL for the Power BI connection

The Dockerfile downloads an ARM64 version of Eclipse Temurin JDK 17, so the current setup targets an ARM64 container environment.

### Step 1 — Prepare environment variables

Copy the example environment file:

```bash
cp .env.example .env
```

Review the variables and update credentials as needed.

Do not commit `.env` or real credentials to the repository. For a public repository, use placeholder values in `.env.example` and document which values readers must configure.

The Compose configuration supplies Docker service names for database connections inside the container network. The host-facing connection addresses used by desktop tools such as Power BI are different: use `localhost` with the published PostgreSQL port.

### Step 2 — Download the dataset

Download the original Online Retail Excel file from the UCI Machine Learning Repository and place it at:

```text
data/raw/online_retail.xlsx
```

The dataset is intentionally excluded from the repository.

### Step 3 — Build and start the containers

From the project root:

```bash
docker compose build --no-cache
docker compose up -d
docker compose ps
```

Wait for the database and Airflow services to finish initializing before proceeding.

### Step 4 — Load the Excel data into MySQL

Run:

```bash
docker exec -it retail_airflow_webserver \
  python /opt/airflow/src/01_excel_to_mysql.py
```

This loads the Excel records into the MySQL source table. Because the script truncates the target table before loading, run it only when you need to initialize or reload the source data.

### Step 5 — Trigger the Airflow DAG

Open Airflow in a browser:

```text
http://localhost:8080
```

Use the credentials configured in the local Compose setup. Change default credentials before using the project in any shared or exposed environment.

In the Airflow UI:

1. Find `retail_production_pipeline`.
2. Trigger the DAG manually.
3. Monitor the three tasks.
4. Confirm that all tasks finish successfully.

The successful execution documented for this project was a manual trigger.

### Step 6 — Open the Power BI dashboard

Open:

```text
bi/Retail_Sales_Performance.pbix
```

Configure the PostgreSQL ODBC connection to use the local database:

| Setting | Value |
|---|---|
| Host | `localhost` |
| Port | `5432` |
| Database | `retail_dwh` |
| Schema | `mart` |

Use the credentials configured in your local environment. If the published host port or database credentials have been changed, update the connection settings accordingly.

### Step 7 — Stop the environment

To stop the running containers while retaining their data volumes:

```bash
docker compose stop
```

To remove the containers and network:

```bash
docker compose down
```

Named volumes are retained by default. Avoid `docker compose down -v` unless you intentionally want to delete the persistent database data and rebuild the environment from scratch.

## 10. Project Structure

```text
.
├── dags/
│   └── retail_etl_dag.py
├── src/
│   ├── 01_excel_to_mysql.py
│   └── 02_spark_mysql_to_postgres.py
├── sql/
│   ├── 00_init_mysql_source.sql
│   ├── 00_init_airflow_db.sql
│   ├── 01_init_postgres_dwh.sql
│   └── 02_build_postgres_mart.sql
├── data/
│   └── raw/
│       └── online_retail.xlsx   # Download locally; not committed
├── bi/
│   └── Retail_Sales_Performance.pbix
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

## 11. Results

The following row counts were observed in PostgreSQL after a successful DAG run:

| Table | Observed rows |
|---|---:|
| `warehouse.online_retail_clean` | 541,909 |
| `mart.dim_date` | 374 |
| `mart.dim_customer` | 4,373 |
| `mart.dim_product` | 4,070 |
| `mart.fact_sales` | 397,884 |
| `mart.agg_daily_sales` | 305 |

These are observed results from the tested dataset and run, not guaranteed counts for every future execution.

The project successfully demonstrated the end-to-end flow from Excel ingestion through MySQL, PySpark transformation, PostgreSQL warehouse and data mart creation, Airflow orchestration, and Power BI reporting.

## 12. Current Scope and Limitations

- The project runs in a local development environment.
- Excel-to-MySQL ingestion is executed separately from the Airflow DAG.
- The DAG's manual execution was verified; automatic scheduled execution has not been verified.
- The warehouse and mart are rebuilt by the DAG rather than incrementally updated.
- Table row counts were checked after a successful run, but a comprehensive automated data-quality test suite has not been implemented.
- Returns are retained in the product fact table when they pass the other mart filters, so negative quantities can contribute negative revenue.
- The DAX calculated columns `Market Segment` and `Day Type` should be checked against the Boolean data types created by the PostgreSQL SQL.
- This project is a personal learning and portfolio project, not a production deployment.

## 13. Potential Future Improvements

Possible future enhancements include:

- Add automated data-quality checks and validation queries.
- Add pipeline-level logging and more detailed failure diagnostics.
- Implement incremental loading instead of dropping and rebuilding downstream tables.
- Verify and test scheduled DAG execution and timezone behavior.
- Add documented test cases for cancellations, returns, missing customer IDs, and invalid prices.
- Review and validate Power BI calculated columns and filter-context behavior.

---

**Project focus:** Data ingestion, ETL orchestration, PySpark transformation, PostgreSQL dimensional modeling, and Power BI analytics.