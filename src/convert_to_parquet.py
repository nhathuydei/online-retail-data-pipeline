import time
from pathlib import Path
import pandas as pd

# Xác định đường dẫn gốc (ROOT) phù hợp với cấu trúc dự án của bạn
ROOT = Path(__file__).resolve().parents[1]  # Hoặc parents[0] tùy vị trí file của bạn
EXCEL_PATH = ROOT / "data" / "raw" / "online_retail.xlsx"
PARQUET_PATH = ROOT / "data" / "raw" / "online_retail.parquet"

print("⏳ Đang chuyển đổi file Excel sang Parquet (chỉ chạy duy nhất lần này)...")
start_time = time.time()

# 1. Đọc file Excel dùng engine calamine để đạt tốc độ nhanh (~7 giây)
df = pd.read_excel(EXCEL_PATH, engine="calamine")

# 2. Sửa lỗi định dạng hỗn hợp (Mixed Types) gây lỗi PyArrow
# Ép toàn bộ các cột văn bản về kiểu chuỗi thuần túy, các ô trống sẽ chuyển thành chuỗi rỗng hoặc None
text_columns = ["InvoiceNo", "StockCode", "Description", "CustomerID", "Country"]
for col in text_columns:
    if col in df.columns:
        # Chuyển đổi sang kiểu string của pandas giúp pyarrow hiểu đúng là cột Text
        df[col] = df[col].astype(str).str.strip()
        # Đưa các giá trị rác hệ thống sinh ra từ ô trống về lại đúng định dạng rỗng chuẩn
        df[col] = df[col].replace({"nan": None, "None": None, "": None})

# 3. Ghi dữ liệu ra định dạng Parquet siêu nhẹ, siêu nhanh
df.to_parquet(PARQUET_PATH, index=False, engine="pyarrow")

print(f"🎉 Hoàn thành! File Parquet đã được lưu tại: {PARQUET_PATH}")
print(f"⏱️  Thời gian xử lý: {time.time() - start_time:.2f} giây.")
