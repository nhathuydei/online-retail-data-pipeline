import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ============================================================
# 0. LOAD ENVIRONMENT VARIABLES
# ============================================================
load_dotenv()

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
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

# ============================================================
# 2. EXTRACT FROM MYSQL (Source Layer)
# ============================================================
mysql_jdbc_url = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?useSSL=false&allowPublicKeyRetrieval=true"

print(f"Extracting raw data from MySQL: {MYSQL_DB}.raw_sales...")

df = (
    spark.read
    .format("jdbc")
    .option("url", mysql_jdbc_url)
    .option("dbtable", "raw_sales")
    .option("user", MYSQL_USER)
    .option("password", MYSQL_PASSWORD)
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("pushDownPredicate", "false")
    .load()
)

# ============================================================
# 3. CLEANING & FILTERING
# ============================================================
print("Transforming and standardizing data...")

# A. Remove test records
df = df.filter(~F.col("Description").ilike("%test%"))

# B. Data Type & Text Standardization
df = df.withColumn(
    "CustomerID",
    F.when(
        (F.col("CustomerID").isNull()) | (F.trim(F.col("CustomerID")) == "") | (F.col("CustomerID") == "None"),
        None
    ).otherwise(F.trim(F.col("CustomerID")))
)

df = df.withColumn("StockCode", F.upper(F.trim(F.col("StockCode"))))
df = df.withColumn("Description", F.trim(F.col("Description")))

# ============================================================
# 4. DERIVE TRANSACTION FLAGS
# ============================================================
df = df.withColumn("IsCanceled", F.col("InvoiceNo").startswith("C"))
df = df.withColumn("IsReturn", F.col("Quantity") < 0)
df = df.withColumn("IsFreeOrInvalidPrice", F.col("UnitPrice") <= 0)
df = df.withColumn("IsBadDebtAdjustment", F.col("InvoiceNo").startswith("A"))

# ============================================================
# 5. CUSTOMER CLASSIFICATION
# ============================================================
df = df.withColumn("HasCustomer", F.col("CustomerID").isNotNull())
df = df.withColumn(
    "CustomerType",
    F.when(F.col("HasCustomer"), "Identified").otherwise("Unknown")
)

# ============================================================
# 6. VALID TRANSACTION FLAG
# ============================================================
df = df.withColumn(
    "IsValidTransaction",
    F.col("Quantity").isNotNull() &
    F.col("UnitPrice").isNotNull() &
    (F.col("Quantity") != 0) &
    (F.col("UnitPrice") > 0) &
    ~F.col("IsCanceled") &
    ~F.col("IsBadDebtAdjustment")
)

# ============================================================
# 7. PRODUCT CLASSIFICATION
# ============================================================
df = df.withColumn(
    "ProductType",
    F.when(F.col("StockCode").isin("POST", "C2"), "Shipping")
    .when(F.col("StockCode") == "DOT", "Discount")
    .when(F.col("StockCode").isin("AMAZONFEE", "BANK CHARGES"), "Fee")
    .when(F.col("StockCode") == "S", "Sample")
    .when(F.col("StockCode") == "M", "Manual")
    .when(F.col("StockCode").startswith("GIFT"), "Gift")
    .when(F.col("StockCode").startswith("DCGS"), "Internal")
    .otherwise("Product")
)

# ============================================================
# 8. CALCULATE REVENUE & AUDIT COLUMN
# ============================================================
df = df.withColumn("Revenue", F.col("Quantity") * F.col("UnitPrice"))
df = df.withColumn("created_at", F.current_timestamp())

# ============================================================
# 9. REMOVE DUPLICATES & LOWERCASE ALL COLUMNS FOR POSTGRES
# ============================================================
df = df.dropDuplicates()

# Chuyển toàn bộ tên cột sang chữ thường để khớp tuyệt đối với DDL Postgres
for col_name in df.columns:
    df = df.withColumnRenamed(col_name, col_name.lower())

# ============================================================
# 10. LOAD TO POSTGRESQL WAREHOUSE
# ============================================================
postgres_jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

postgres_properties = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}

print(f"Loading cleaned data to PostgreSQL: {POSTGRES_DB}.warehouse.online_retail_clean...")

(
    df.write
    .jdbc(
        url=postgres_jdbc_url,
        table="warehouse.online_retail_clean",
        mode="append",
        properties={
            **postgres_properties,
            "truncate": "true"  # Reset dữ liệu sạch trước khi append
        }
    )
)

print("ETL Transform and Load to PostgreSQL DWH completed successfully!")

# ============================================================
# 11. STOP SPARK
# ============================================================
spark.stop()