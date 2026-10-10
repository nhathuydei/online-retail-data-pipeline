CREATE DATABASE IF NOT EXISTS retail_source
    DEFAULT CHARACTER SET utf8mb4
    DEFAULT COLLATE utf8mb4_unicode_ci;

USE retail_source;

-- 2. Khởi tạo bảng raw_sales với kiểu dữ liệu chuẩn cho tầng Raw Staging
DROP TABLE IF EXISTS raw_sales;

CREATE TABLE raw_sales (
    InvoiceNo VARCHAR(50),
    StockCode VARCHAR(50),
    Description TEXT,              -- Dùng TEXT vì một số mô tả sản phẩm có thể khá dài
    Quantity INT,
    InvoiceDate DATETIME,
    UnitPrice DECIMAL(12, 4),      -- 4 chữ số thập phân để tránh mất độ chính xác khi tính toán
    CustomerID VARCHAR(50),        -- ĐỔI THÀNH VARCHAR để tránh bị dính đuôi .00
    Country VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Cột audit ghi nhận thời gian nạp data
);