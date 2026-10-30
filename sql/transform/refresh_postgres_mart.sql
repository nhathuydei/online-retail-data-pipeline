-- =========================================================
-- ETL SCRIPT: Nạp dữ liệu vào Data Mart (Chạy định kỳ qua Airflow)
-- =========================================================

-- Bước 1: Nạp Dim Date (Full reload hoặc sinh bổ sung dải ngày)
TRUNCATE TABLE mart.dim_date CASCADE;
WITH date_boundaries AS (
    SELECT MIN(DATE(invoicedate)) AS min_date, MAX(DATE(invoicedate)) AS max_date 
    FROM mart.vw_sales_base
) 
INSERT INTO mart.dim_date (date_key, year, month, day, year_month, day_of_week_num, day_of_week_name, is_weekend)
SELECT 
    d::DATE AS date_key,
    EXTRACT(YEAR FROM d)::INT AS year,
    EXTRACT(MONTH FROM d)::INT AS month,
    EXTRACT(DAY FROM d)::INT AS day,
    TO_CHAR(d, 'YYYY-MM') AS year_month,
    EXTRACT(DOW FROM d)::INT AS day_of_week_num,
    TO_CHAR(d, 'FMDay') AS day_of_week_name,
    CASE WHEN EXTRACT(DOW FROM d) IN (0, 6) THEN true ELSE false END AS is_weekend 
FROM date_boundaries, GENERATE_SERIES(min_date, max_date, INTERVAL '1 day') AS d;


-- Bước 2: Nạp Dim Product (Sử dụng UPSERT / ON CONFLICT để tối ưu nếu chạy incremental)
TRUNCATE TABLE mart.dim_product CASCADE;
INSERT INTO mart.dim_product (stockcode, description, producttype)
SELECT DISTINCT ON (stockcode) stockcode, description, producttype 
FROM mart.vw_sales_base 
WHERE producttype = 'Product' 
ORDER BY stockcode, LENGTH(description) DESC;


-- Bước 3: Nạp Dim Customer
TRUNCATE TABLE mart.dim_customer CASCADE;
INSERT INTO mart.dim_customer (customerid, customertype, firstpurchasedate, lastpurchasedate)
SELECT 
    COALESCE(customerid, 'UNKNOWN') AS customerid, 
    MIN(customertype) AS customertype, 
    MIN(invoicedate) AS firstpurchasedate, 
    MAX(invoicedate) AS lastpurchasedate 
FROM mart.vw_sales_base 
GROUP BY COALESCE(customerid, 'UNKNOWN');


-- Bước 4: Nạp Fact Sales (Sử dụng Refresh toàn bộ hoặc Partition Overwrite theo ngày)
TRUNCATE TABLE mart.fact_sales CASCADE;
INSERT INTO mart.fact_sales (invoiceno, stockcode, customerid, date_key, quantity, revenue, producttype, is_domestic)
SELECT 
    invoiceno, 
    stockcode, 
    COALESCE(customerid, 'UNKNOWN') AS customerid, 
    DATE(invoicedate) AS date_key, 
    quantity, 
    revenue, 
    producttype, 
    CASE WHEN country = 'United Kingdom' THEN true ELSE false END AS is_domestic 
FROM mart.vw_sales_base 
WHERE producttype = 'Product';


-- Bước 5: Nạp Aggregate Table
TRUNCATE TABLE mart.agg_daily_sales;
INSERT INTO mart.agg_daily_sales (date_key, total_revenue, total_quantity)
SELECT 
    date_key, 
    SUM(revenue) AS total_revenue, 
    SUM(quantity) AS total_quantity 
FROM mart.fact_sales 
GROUP BY date_key;