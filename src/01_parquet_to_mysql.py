import argparse
import os
import sys
import time
from pathlib import Path
import mysql.connector
import pandas as pd
from dotenv import load_dotenv

# =========================================================
# 1. PATHS AND ENVIRONMENT
# =========================================================
ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

# 🔄 ĐÃ ĐỔI: Chuyển đường dẫn từ file .xlsx sang .parquet
DATA_PATH = ROOT / "data" / "raw" / "online_retail.parquet"

MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "127.0.0.1"),
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", ""),
    "database": os.getenv("MYSQL_DB", "retail_source"),
}
BATCH_SIZE = 1000
COLUMNS = [
    "InvoiceNo",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "UnitPrice",
    "CustomerID",
    "Country",
]

# =========================================================
# 2. LOAD DATA & PROCESS LOGGING
# =========================================================
def load_excel():
    if not DATA_PATH.exists():
        raise FileNotFoundError(
            f"Không tìm thấy file dữ liệu Parquet tại: {DATA_PATH}\n"
            "Hãy chạy file convert_to_parquet.py trước."
        )
    
    print("\n--- [1/3] BẮT ĐẦU ĐỌC VÀ CHUẨN HÓA DỮ LIỆU ---")
    start_time = time.time()
    
    # 🔄 ĐÃ ĐỔI: Đọc bằng read_parquet thay vì read_excel
    df = pd.read_parquet(DATA_PATH)
    
    print(f"👉 Đọc thành công file nền Parquet: {len(df):,} dòng ({time.time() - start_time:.2f}s)")

    # Chuẩn hóa tên cột
    df.columns = df.columns.str.strip()
    missing_columns = set(COLUMNS) - set(df.columns)
    if missing_columns:
        raise ValueError(
            f"Dữ liệu thiếu các cột bắt buộc: {sorted(missing_columns)}"
        )

    # Chỉ lấy 8 cột nghiệp vụ
    df = df[COLUMNS].copy()

    # 1. Chuẩn hóa ngày (File Parquet thường giữ nguyên định dạng datetime nên bước này sẽ chạy siêu tốc)
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors="coerce")
    invalid_dates = df["InvoiceDate"].isna().sum()
    if invalid_dates:
        print(f"⚠️  Loại bỏ: {invalid_dates:,} dòng trống hoặc sai định dạng 'InvoiceDate'.")
        df = df.dropna(subset=["InvoiceDate"])

    # 2. Chuẩn hóa CustomerID thành chuỗi, tránh giá trị dạng '12346.0'
    def normalize_customer_id(value):
        if pd.isna(value):
            return None
        value = str(value).strip()
        if not value or value.lower() == "none":
            return None
        if value.endswith(".0"):
            value = value[:-2]
        return value
    
    df["CustomerID"] = df["CustomerID"].apply(normalize_customer_id)
    final_null_cust = df["CustomerID"].isna().sum()
    print(f"🔄 Chuẩn hóa: Đã kiểm tra CustomerID (Hiện có {final_null_cust:,} dòng trống ID).")

    # 3. Chuẩn hóa các cột văn bản
    for col in ["InvoiceNo", "StockCode", "Description", "Country"]:
        df[col] = df[col].apply(lambda value: None if pd.isna(value) else str(value).strip())

    # 4. Ép kiểu số
    df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce")
    df["UnitPrice"] = pd.to_numeric(df["UnitPrice"], errors="coerce")

    invalid_numeric = df["Quantity"].isna() | df["UnitPrice"].isna()
    if invalid_numeric.any():
        print(f"⚠️  Loại bỏ: {invalid_numeric.sum():,} dòng có Quantity hoặc UnitPrice không hợp lệ.")
        df = df.loc[~invalid_numeric].copy()

    if not (df["Quantity"] % 1 == 0).all():
        raise ValueError("Có Quantity không phải số nguyên, không thể nạp vào cột MySQL INT.")
        
    df["Quantity"] = df["Quantity"].astype("int64")
    print(f"✅ Hoàn tất chuẩn hóa. Số lượng dữ liệu sạch sẵn sàng: {len(df):,} dòng.")
    return df

# =========================================================
# 3. SELECT DATA BY MODE
# =========================================================
def select_data(df, mode, month=None):
    print("\n--- [2/3] PHÂN LỌC DỮ LIỆU THEO CHẾ ĐỘ NẠP ---")
    if mode == "initial":
        selected = df[df["InvoiceDate"] < pd.Timestamp("2011-01-01")].copy()
        description = "Dữ liệu lịch sử trước năm 2011-01"
    else:
        start_date = pd.Timestamp(f"{month}-01")
        end_date = start_date + pd.DateOffset(months=1)
        selected = df[(df["InvoiceDate"] >= start_date) & (df["InvoiceDate"] < end_date)].copy()
        description = f"Dữ liệu của riêng tháng {month}"

    print(f"📋 Chế độ chọn: {description}")
    print(f"📊 Số lượng bản ghi lọc được: {len(selected):,} dòng.")
    return selected

# =========================================================
# 4. INSERT INTO MYSQL
# =========================================================
def insert_to_mysql(df):
    if df.empty:
        print("❌ Không có dữ liệu nào phù hợp trong phạm vi được chọn để nạp.")
        return

    print("\n--- [3/3] ĐANG GHI DỮ LIỆU VÀO MYSQL DB ---")
    connection = mysql.connector.connect(**MYSQL_CONFIG)
    insert_sql = """
        INSERT INTO raw_sales (
            InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    total_rows = len(df)
    total_inserted = 0
    start_time = time.time()

    try:
        cursor = connection.cursor()
        for start in range(0, total_rows, BATCH_SIZE):
            batch_df = df.iloc[start:start + BATCH_SIZE]
            records = []
            for row in batch_df.itertuples(index=False, name=None):
                (invoice_no, stock_code, description, quantity, invoice_date, unit_price, customer_id, country) = row
                records.append((
                    invoice_no, stock_code, description, int(quantity),
                    invoice_date.to_pydatetime(), round(float(unit_price), 4),
                    customer_id, country
                ))
            
            cursor.executemany(insert_sql, records)
            connection.commit()
            total_inserted += len(records)
            
            percent = (total_inserted / total_rows) * 100
            sys.stdout.write(f"\r🚀 Tiến trình nạp: [{total_inserted:,}/{total_rows:,}] Dòng (~{percent:.1f}%)")
            sys.stdout.flush()

        print(f"\n\n🎉 THÀNH CÔNG! Đã nạp xong {total_inserted:,} dòng vào bảng 'raw_sales'.")
        print(f"⏱️  Tổng thời gian thực thi: {time.time() - start_time:.2f} giây.")

    except Exception:
        connection.rollback()
        print("\n❌ Thất bại! Đã xảy ra lỗi hệ thống, rollback dữ liệu...")
        raise
    finally:
        connection.close()

# =========================================================
# 5. MAIN
# =========================================================
def main():
    parser = argparse.ArgumentParser(description="Nạp dữ liệu Online Retail từ Parquet vào MySQL Source.")
    parser.add_argument("--mode", choices=["initial"], help="Nạp dữ liệu lịch sử trước năm 2011.")
    parser.add_argument("--monthly", type=str, help="Nạp dữ liệu theo tháng, định dạng YYYY-MM.")
    args = parser.parse_args()

    if bool(args.mode) == bool(args.monthly):
        parser.error("Chọn đúng một chế độ: --mode initial hoặc --monthly YYYY-MM.")

    if args.monthly:
        try:
            parsed_month = pd.Period(args.monthly, freq="M")
        except (ValueError, TypeError):
            parser.error("--monthly phải có định dạng YYYY-MM.")
        if parsed_month < pd.Period("2011-01", freq="M"):
            parser.error("Chế độ --monthly chỉ dùng cho tháng 01/2011 trở đi.")
        month = str(parsed_month)
        mode = "monthly"
    else:
        month = None
        mode = "initial"

    print(f"Đọc dữ liệu từ: {DATA_PATH}")
    
    df = load_excel()
    selected = select_data(df, mode, month)
    insert_to_mysql(selected)

if __name__ == "__main__":
    main()