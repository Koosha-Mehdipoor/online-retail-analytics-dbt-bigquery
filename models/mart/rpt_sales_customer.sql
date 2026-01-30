{{
config(
    materialized='view',
    tags=['mart']
)
}}

SELECT *, 
c.first_order_date,
c.last_order_date,
c.total_value,
c.total_orders,
c.total_quantity,
c.avg_order_value

FROM {{ ref ('f_online_retail') }}  AS f
LEFT JOIN {{ ref ('dim_customers') }} AS c
    ON f.customer_id = c.customer_id
WHERE d.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
