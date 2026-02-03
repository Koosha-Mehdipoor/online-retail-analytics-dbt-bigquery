{{
config(
    materialized='view',
    tags=['mart']
)
}}
WITH fact AS (
  SELECT
    invoice_date_id,
    stock_code,
    SUM(quantity) AS total_quantity,
    SUM(sales_amount) AS total_sales_amount,
    SUM(return_amount) AS total_return_amount,
    SUM(net_amount) AS total_net_amount
FROM {{ref ('f_online_retail') }}
GROUP BY stock_code,invoice_date_id

)
SELECT
f.*,
 d.date AS invoice_date,
 p.description,
 p.first_sold_date,
 p.last_sold_date,
 p.unit_price,
 p.total_orders,
 p.total_registered_customers,
 p.total_guest_customers,
 p.total_quantity_sold
FROM fact AS f
JOIN {{ ref ('dim_products') }} AS p
    ON f.stock_code = p.stock_code
JOIN {{ ref('dim_dates') }} AS d
    ON f.invoice_date_id = d.date_id
WHERE d.date >= DATE_SUB((SELECT MAX(invoice_date) FROM {{ ref('stg_online_retail') }}), INTERVAL 1 YEAR)