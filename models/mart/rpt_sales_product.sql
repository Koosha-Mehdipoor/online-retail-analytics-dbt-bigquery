{{
config(
    materialized='view',
    tags=['mart']
)
}}

SELECT f.*,
 p.description,
 p.first_sold_date,
 p.last_sold_date,
 p.unit_price,
 p.total_orders,
 p.total_registered_customers,
 p.total_guest_orders,
 p.total_quantity_sold
FROM {{ref ('f_online_retail') }} AS f
JOIN {{ ref ('dim_products') }} AS p
    ON f.stock_code = p.stock_code
WHERE f.invoice_date_id >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)