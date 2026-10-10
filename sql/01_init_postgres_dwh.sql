-- ============================================================
-- Step 1: Initialize Database (Chạy script này khi ở DB 'postgres')
-- ============================================================
-- CREATE DATABASE retail_dwh;

-- Switch connection to retail_dwh (psql: \c retail_dwh)

-- ============================================================
-- Step 2: Initialize Schema & Clean Table
-- ============================================================
CREATE SCHEMA IF NOT EXISTS warehouse;

DROP TABLE IF EXISTS warehouse.online_retail_clean CASCADE;

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
CHECK (producttype IN (
    'Product', 'Shipping', 'Discount', 'Fee', 
    'Sample', 'Manual', 'Gift', 'Internal'
));

ALTER TABLE warehouse.online_retail_clean
ADD CONSTRAINT chk_customertype
CHECK (customertype IN ('Identified', 'Unknown'));

-- ============================================================
-- Step 4: Create Indexes for Performance
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_clean_invoice ON warehouse.online_retail_clean(invoiceno);
CREATE INDEX IF NOT EXISTS idx_clean_customer ON warehouse.online_retail_clean(customerid);
CREATE INDEX IF NOT EXISTS idx_clean_date ON warehouse.online_retail_clean(invoicedate);
CREATE INDEX IF NOT EXISTS idx_clean_producttype ON warehouse.online_retail_clean(producttype);