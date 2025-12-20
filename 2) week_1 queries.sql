CREATE DATABASE retail_project1;
USE retail_project1;

CREATE TABLE sales_raw1 (
  invoice_no VARCHAR(20),
  customer_id INT,
  product_id INT,
  product_name VARCHAR(100),
  category VARCHAR(50),
  quantity INT,
  unit_price DECIMAL(10,2),
  invoice_date DATE,
  region VARCHAR(50)
);

SELECT * FROM sales_raw1 LIMIT 10;

select count(*) from sales_raw1;
CREATE TABLE dim_customer AS
SELECT DISTINCT
  customer_id,
  region
FROM sales_raw1;

CREATE TABLE dim_product AS
SELECT DISTINCT
  product_id,
  product_name,
  category
FROM sales_raw1;

CREATE TABLE fact_sales AS
SELECT
  invoice_no,
  customer_id,
  product_id,
  quantity,
  quantity * unit_price AS revenue,
  invoice_date
FROM sales_raw1;

SELECT COUNT(*) FROM fact_sales;
SELECT COUNT(DISTINCT customer_id) FROM dim_customer;


CREATE VIEW customer_360 AS
SELECT
  c.customer_id,
  c.region,
  p.product_name,
  p.category,
  f.invoice_date,
  f.quantity,
  f.revenue
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
JOIN dim_product p ON f.product_id = p.product_id;


SELECT * FROM customer_360 LIMIT 199;

SELECT
  customer_id,
  SUM(revenue) AS total_spent
FROM customer_360
GROUP BY customer_id;

SELECT
  customer_id,
  COUNT(DISTINCT invoice_no) AS total_orders
FROM fact_sales
GROUP BY customer_id;

SELECT
  customer_id,
  MAX(invoice_date) AS last_purchase_date
FROM fact_sales
GROUP BY customer_id;


CREATE TABLE rfm_base AS
SELECT
  customer_id,
  DATEDIFF(CURDATE(), MAX(invoice_date)) AS recency,
  COUNT(DISTINCT invoice_no) AS frequency,
  SUM(revenue) AS monetary
FROM fact_sales
GROUP BY customer_id;

SELECT *,
CASE
  WHEN monetary > 500 THEN 'High Value'
  WHEN monetary BETWEEN 300 AND 1000 THEN 'Medium Value'
  ELSE 'Low Value'
END AS customer_segment
FROM rfm_base;
SELECT customer_id, invoice_date, revenue
FROM customer_360;

SELECT customer_id, invoice_date, product_name
FROM customer_360;

