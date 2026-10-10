import os
import pandas as pd
import mysql.connector

# Cấu hình đường dẫn & thông số DB
EXCEL_PATH = "/opt/airflow/data/raw/online_retail.xlsx"
MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql_source")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "root")
MYSQL_DB = os.getenv("MYSQL_DB", "retail_source")

print("--- Step 1: Ingesting Excel to MySQL (retail_source) ---")
print(f"Reading file: {EXCEL_PATH}...")

df = pd.read_excel(EXCEL_PATH)

# Làm sạch dữ liệu cơ bản trước khi nạp vào MySQL
df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate']).dt.strftime('%Y-%m-%d %H:%M:%S')
df['CustomerID'] = df['CustomerID'].astype(str).replace({'nan': None, 'None': None})
df['Description'] = df['Description'].astype(str).replace({'nan': None, 'None': None})

# Kết nối MySQL
conn = mysql.connector.connect(
    host=MYSQL_HOST,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database=MYSQL_DB,
    port=3306
)
cursor = conn.cursor()

print("Clearing existing records in raw_sales...")
cursor.execute("TRUNCATE TABLE raw_sales;")
conn.commit()

# Lệnh Insert
insert_query = """
INSERT INTO raw_sales (InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

# Chia nhỏ chèn theo Batch (Chunk size: 20,000 dòng/lần)
BATCH_SIZE = 20000
data_tuples = [tuple(x) for x in df[['InvoiceNo', 'StockCode', 'Description', 'Quantity', 'InvoiceDate', 'UnitPrice', 'CustomerID', 'Country']].to_numpy()]
total_rows = len(data_tuples)

print(f"Inserting {total_rows} records into MySQL in batches of {BATCH_SIZE}...")

for i in range(0, total_rows, BATCH_SIZE):
    batch = data_tuples[i:i + BATCH_SIZE]
    cursor.executemany(insert_query, batch)
    conn.commit()
    print(f"  -> Inserted {min(i + BATCH_SIZE, total_rows)}/{total_rows} rows...")

cursor.close()
conn.close()
print("Successfully seeded data into MySQL!")