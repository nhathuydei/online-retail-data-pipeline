import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# 0. LOAD ENVIRONMENT VARIABLES
# ============================================================

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3306")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DB = os.getenv("MYSQL_DB", "retail_source")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
POSTGRES_DB = os.getenv("POSTGRES_DB", "retail_dwh")


# ============================================================
# 1. CREATE SPARK SESSION
# ============================================================

print("--- Step 2: Running PySpark ETL Transform ---")

spark = (
    SparkSession.builder
    .appName("Retail ETL Pipeline")
    .config(
        "spark.jars.packages",
        "mysql:mysql-connector-java:8.0.33,"
        "org.postgresql:postgresql:42.7.3"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


try:
    # ========================================================
    # 2. EXTRACT FROM MYSQL SOURCE
    # ========================================================

    mysql_jdbc_url = (
        f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
        "?useSSL=false&allowPublicKeyRetrieval=true"
    )

    print(f"Extracting data from MySQL: {MYSQL_DB}.raw_sales")

    source_query = """
        (
            SELECT
                InvoiceNo,
                StockCode,
                Description,
                Quantity,
                InvoiceDate,
                UnitPrice,
                CustomerID,
                Country,
                created_at
            FROM raw_sales
        ) AS source_data
    """

    df = (
        spark.read
        .format("jdbc")
        .option("url", mysql_jdbc_url)
        .option("dbtable", source_query)
        .option("user", MYSQL_USER)
        .option("password", MYSQL_PASSWORD)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .load()
    )

    print("Columns read from MySQL:", df.columns)
    df.printSchema()

    source_count = df.count()
    print(f"Rows extracted: {source_count:,}")


    # ========================================================
    # 3. CLEANING AND STANDARDIZATION
    # ========================================================

    print("Transforming and standardizing data...")

    # A. Remove test records; preserve rows with NULL Description.
    df = df.filter(
        F.col("Description").isNull()
        | ~F.lower(F.col("Description")).contains("test")
    )

    # B. Standardize CustomerID.
    df = df.withColumn(
        "CustomerID",
        F.when(
            F.col("CustomerID").isNull()
            | (F.trim(F.col("CustomerID")) == "")
            | (F.trim(F.col("CustomerID")) == "None"),
            F.lit(None).cast("string")
        ).otherwise(F.trim(F.col("CustomerID")))
    )

    # C. Standardize product code and description.
    df = df.withColumn(
        "StockCode",
        F.upper(F.trim(F.col("StockCode")))
    )

    df = df.withColumn(
        "Description",
        F.trim(F.col("Description"))
    )


    # ========================================================
    # 4. DERIVE TRANSACTION FLAGS
    # ========================================================

    df = df.withColumn(
        "IsCanceled",
        F.coalesce(F.col("InvoiceNo").startswith("C"), F.lit(False))
    )

    df = df.withColumn(
        "IsReturn",
        F.col("Quantity") < 0
    )

    df = df.withColumn(
        "IsFreeOrInvalidPrice",
        F.col("UnitPrice") <= 0
    )

    df = df.withColumn(
        "IsBadDebtAdjustment",
        F.coalesce(F.col("InvoiceNo").startswith("A"), F.lit(False))
    )


    # ========================================================
    # 5. CUSTOMER CLASSIFICATION
    # ========================================================

    df = df.withColumn(
        "HasCustomer",
        F.col("CustomerID").isNotNull()
    )

    df = df.withColumn(
        "CustomerType",
        F.when(F.col("HasCustomer"), "Identified")
        .otherwise("Unknown")
    )


    # ========================================================
    # 6. VALID TRANSACTION FLAG
    # ========================================================

    df = df.withColumn(
        "IsValidTransaction",
        F.col("Quantity").isNotNull()
        & F.col("UnitPrice").isNotNull()
        & (F.col("Quantity") != 0)
        & (F.col("UnitPrice") > 0)
        & ~F.col("IsCanceled")
        & ~F.col("IsBadDebtAdjustment")
    )


    # ========================================================
    # 7. PRODUCT CLASSIFICATION
    # ========================================================

    df = df.withColumn(
        "ProductType",
        F.when(
            F.col("StockCode").isin("POST", "C2"),
            "Shipping"
        )
        .when(F.col("StockCode") == "DOT", "Discount")
        .when(
            F.col("StockCode").isin("AMAZONFEE", "BANK CHARGES"),
            "Fee"
        )
        .when(F.col("StockCode") == "S", "Sample")
        .when(F.col("StockCode") == "M", "Manual")
        .when(F.col("StockCode").startswith("GIFT"), "Gift")
        .when(F.col("StockCode").startswith("DCGS"), "Internal")
        .otherwise("Product")
    )


    # ========================================================
    # 8. CALCULATE REVENUE
    # ========================================================

    df = df.withColumn(
        "Revenue",
        F.col("Quantity") * F.col("UnitPrice")
    )

    # Keep created_at from MySQL.
    # Do not overwrite it with the Spark execution timestamp.


    # ========================================================
    # 9. REMOVE EXACT DUPLICATES AND STANDARDIZE COLUMN NAMES
    # ========================================================

    df = df.dropDuplicates()

    for col_name in df.columns:
        df = df.withColumnRenamed(
            col_name,
            col_name.lower()
        )

    transformed_count = df.count()
    print(f"Rows after transformation: {transformed_count:,}")


    # ========================================================
    # 10. LOAD TO POSTGRESQL DATA WAREHOUSE
    # ========================================================

    postgres_jdbc_url = (
        f"jdbc:postgresql://{POSTGRES_HOST}:"
        f"{POSTGRES_PORT}/{POSTGRES_DB}"
    )

    postgres_properties = {
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
    }

    target_table = "warehouse.online_retail_clean"

    print(
        f"Loading data to PostgreSQL: "
        f"{POSTGRES_DB}.{target_table}"
    )

    (
        df.write
        .mode("overwrite")
        .option("truncate", "true")
        .jdbc(
            url=postgres_jdbc_url,
            table=target_table,
            properties=postgres_properties,
        )
    )

    print(
        "ETL completed successfully. "
        f"Loaded {transformed_count:,} rows into {target_table}."
    )

finally:
    spark.stop()