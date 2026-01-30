{{
config(
    materialized='view',
    tags=['mart']
)
}}

SELECT
f.row_hash,
f.invoice_no,
f.stock_code,
f.quantity,
f.order_status,
f.customer_id,
f.country,
f.sales_amount,
f.return_amount,
f.net_amount,
 d.date AS invoice_date,
 p.description,
 p.first_sold_date,
 p.last_sold_date,
 p.unit_price,
 p.total_orders,
 p.total_registered_customers,
 p.total_guest_customers,
 p.total_quantity_sold
FROM {{ref ('f_online_retail') }} AS f
JOIN {{ ref ('dim_products') }} AS p
    ON f.stock_code = p.stock_code
JOIN {{ ref('dim_dates') }} AS d
    ON f.invoice_date_id = d.date_id
WHERE d.date >= DATE_SUB((SELECT MAX(invoice_date) FROM {{ ref('stg_online_retail') }}), INTERVAL 1 YEAR)