DROP SCHEMA IF EXISTS mart CASCADE;
CREATE SCHEMA mart;

-- =========================================================
-- 1. BASE VIEW
-- =========================================================
CREATE OR REPLACE VIEW mart.vw_sales_base AS
SELECT * 
FROM warehouse.online_retail_clean orc
WHERE iscanceled = false 
  AND isbaddebtadjustment = false 
  AND producttype NOT IN ('Manual', 'Sample', 'Internal');

-- =========================================================
-- 2. DIM DATE (Continuous Date Series)
-- =========================================================
CREATE TABLE mart.dim_date AS
WITH date_boundaries AS (
    SELECT 
        MIN(DATE(invoicedate)) AS min_date,
        MAX(DATE(invoicedate)) AS max_date
    FROM mart.vw_sales_base
)
SELECT 
    d::DATE AS date_key,
    EXTRACT(YEAR FROM d)::INT AS year,
    EXTRACT(MONTH FROM d)::INT AS month,
    EXTRACT(DAY FROM d)::INT AS day,
    TO_CHAR(d, 'YYYY-MM') AS year_month,
    EXTRACT(DOW FROM d)::INT AS day_of_week_num,
    TO_CHAR(d, 'FMDay') AS day_of_week_name,
    CASE WHEN EXTRACT(DOW FROM d) IN (0, 6) THEN true ELSE false END AS is_weekend
FROM date_boundaries,
GENERATE_SERIES(min_date, max_date, INTERVAL '1 day') AS d;

ALTER TABLE mart.dim_date ADD CONSTRAINT pk_dim_date PRIMARY KEY (date_key);

-- =========================================================
-- 3. DIM PRODUCT
-- =========================================================
CREATE TABLE mart.dim_product AS
SELECT DISTINCT ON (stockcode)
    stockcode,
    description,
    producttype
FROM mart.vw_sales_base
WHERE producttype = 'Product'
ORDER BY stockcode, LENGTH(description) DESC;

ALTER TABLE mart.dim_product ADD CONSTRAINT pk_dim_product PRIMARY KEY (stockcode);

-- =========================================================
-- 4. DIM CUSTOMER
-- =========================================================
CREATE TABLE mart.dim_customer AS
SELECT 
    COALESCE(customerid, 'UNKNOWN') AS customerid,
    MIN(customertype) AS customertype,
    MIN(invoicedate) AS firstpurchasedate,
    MAX(invoicedate) AS lastpurchasedate
FROM mart.vw_sales_base
GROUP BY COALESCE(customerid, 'UNKNOWN');

ALTER TABLE mart.dim_customer ADD CONSTRAINT pk_dim_customer PRIMARY KEY (customerid);

-- =========================================================
-- 5. FACT SALES (Đồng bộ lọc 'Product' để đảm bảo toàn vẹn)
-- =========================================================
CREATE TABLE mart.fact_sales AS
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
WHERE producttype = 'Product'; -- Đảm bảo hạt dữ liệu đồng nhất với dim_product

-- =========================================================
-- 6. INDEXES & FOREIGN KEYS (Bảo đảm Referential Integrity)
-- =========================================================
CREATE INDEX idx_fact_date ON mart.fact_sales(date_key);
CREATE INDEX idx_fact_customer ON mart.fact_sales(customerid);
CREATE INDEX idx_fact_product ON mart.fact_sales(stockcode);

ALTER TABLE mart.fact_sales 
  ADD CONSTRAINT fk_fact_date FOREIGN KEY (date_key) REFERENCES mart.dim_date(date_key);

ALTER TABLE mart.fact_sales 
  ADD CONSTRAINT fk_fact_customer FOREIGN KEY (customerid) REFERENCES mart.dim_customer(customerid);

ALTER TABLE mart.fact_sales 
  ADD CONSTRAINT fk_fact_product FOREIGN KEY (stockcode) REFERENCES mart.dim_product(stockcode);

-- =========================================================
-- 7. AGGREGATE TABLE (Tùy chọn phục vụ High-level report)
-- =========================================================
CREATE TABLE mart.agg_daily_sales AS
SELECT 
    date_key,
    SUM(revenue) AS total_revenue,
    SUM(quantity) AS total_quantity
FROM mart.fact_sales
GROUP BY date_key;