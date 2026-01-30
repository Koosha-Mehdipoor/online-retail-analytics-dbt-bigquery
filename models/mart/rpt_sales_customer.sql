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
c.first_order_date,
c.last_order_date,
c.total_value,
c.total_orders,
c.total_quantity,
c.avg_order_value

FROM {{ ref ('f_online_retail') }}  AS f
LEFT JOIN {{ ref ('dim_customers') }} AS c
    ON f.customer_id = c.customer_id
JOIN {{ ref('dim_dates') }} AS d
    ON f.invoice_date_id = d.date_id
WHERE d.date >= DATE_SUB((SELECT MAX(invoice_date) FROM {{ ref('stg_online_retail') }}), INTERVAL 1 YEAR)
