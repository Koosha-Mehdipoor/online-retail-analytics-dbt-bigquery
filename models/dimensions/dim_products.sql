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
prod_display AS (
    SELECT 
        stock_code,
        product_display,
        ROW_NUMBER() OVER(PARTITION BY stock_code ORDER BY CASE WHEN LOWER(description) = 'not defined' THEN 1 ELSE 2 END DESC)  AS prn
    FROM {{ ref('stg_online_retail') }}
    WHERE product_display IS NOT NULL
),
dim_product AS (
    SELECT 
    UPPER(TRIM(stock_code)) AS stock_code,
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
SELECT dp.stock_code,UPPER(TRIM(pd.product_display)) AS product_display, dp.first_sold_date, dp.last_sold_date, dp.total_orders, dp.total_registered_customers, dp.total_guest_customers, dp.total_quantity_sold,
rd.description AS product_description FROM dim_product AS dp
LEFT JOIN ranked AS rd
ON dp.stock_code = rd.stock_code AND rd.rn = 1
LEFT JOIN prod_display AS pd 
ON dp.stock_code = pd.stock_code AND pd.prn = 1


