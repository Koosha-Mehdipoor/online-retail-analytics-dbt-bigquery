{{
    config(
        materialized = 'table',
        cluster_by = 'stock_code',
        tags = ['dimensions']
    )
}}


WITH distinct_desc AS (
  SELECT 
  DISTINCT
    stock_code,
    `description`
    
FROM {{ ref('stg_online_retail') }}
), ranked AS (
  SELECT 
      stock_code,
     `description`,
     ROW_NUMBER() OVER(PARTITION BY stock_code ORDER BY CASE WHEN LOWER(`description`) = 'not defined' THEN 2 ELSE 1 END,description) AS rn
     FROM distinct_desc
),
dim_product AS (
    SELECT 
    stock_code,
    MIN(invoice_date) AS first_sold_date,
    MAX(invoice_date) AS last_sold_date,
    COUNT(DISTINCT invoice_no) AS total_orders,
    COUNT(DISTINCT CASE WHEN customer_id != 'Not Registered' THEN customer_id END)AS total_registered_customers,
    COUNT(DISTINCT CASE WHEN customer_id = 'Not Registered' THEN invoice_no END) AS total_guest_customers,
    SUM(quantity) AS total_quantity_sold
FROM {{ ref('stg_online_retail') }}
GROUP BY
    stock_code
)
SELECT dp.*, rd.description AS product_description FROM dim_product AS dp
LEFT JOIN ranked AS rd
ON dp.stock_code = rd.stock_code AND rd.rn = 1


