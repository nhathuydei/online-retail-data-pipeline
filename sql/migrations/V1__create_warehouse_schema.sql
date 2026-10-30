
-- ============================================================
-- Step 1: Initialize Schema
-- Chạy trong database retail_dwh
-- ============================================================

CREATE SCHEMA IF NOT EXISTS warehouse;

-- CẢNH BÁO: Xóa bảng và toàn bộ dữ liệu hiện tại.
DROP TABLE IF EXISTS warehouse.online_retail_clean CASCADE;


-- ============================================================
-- Step 2: Create Clean Table
-- ============================================================

CREATE TABLE warehouse.online_retail_clean (
    invoiceno VARCHAR(50) NOT NULL,
    stockcode VARCHAR(50) NOT NULL,
    description TEXT,
    quantity INTEGER,
    invoicedate TIMESTAMP,
    unitprice NUMERIC(12, 4),
    customerid VARCHAR(50),
    country VARCHAR(100),

    iscanceled BOOLEAN,
    isreturn BOOLEAN,
    isfreeorinvalidprice BOOLEAN,
    isbaddebtadjustment BOOLEAN,

    hascustomer BOOLEAN NOT NULL,
    customertype VARCHAR(20) NOT NULL,
    isvalidtransaction BOOLEAN,
    producttype VARCHAR(30) NOT NULL,

    revenue NUMERIC(21, 4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- ============================================================
-- Step 3: Create Constraints
-- ============================================================

ALTER TABLE warehouse.online_retail_clean
ADD CONSTRAINT chk_producttype
CHECK (
    producttype IN (
        'Product',
        'Shipping',
        'Discount',
        'Fee',
        'Sample',
        'Manual',
        'Gift',
        'Internal'
    )
);

ALTER TABLE warehouse.online_retail_clean
ADD CONSTRAINT chk_customertype
CHECK (
    customertype IN ('Identified', 'Unknown')
);


-- ============================================================
-- Step 4: Create Indexes
-- ============================================================

CREATE INDEX idx_clean_invoice
ON warehouse.online_retail_clean(invoiceno);

CREATE INDEX idx_clean_customer
ON warehouse.online_retail_clean(customerid);

CREATE INDEX idx_clean_date
ON warehouse.online_retail_clean(invoicedate);

CREATE INDEX idx_clean_producttype
ON warehouse.online_retail_clean(producttype);