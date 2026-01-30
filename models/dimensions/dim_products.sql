{{
    config(
        materialized = 'table',
        cluster_by = 'stock_code',
        tags = ['dimensions']
    )
}}


SELECT 
    stock_code,
    description,
    MIN(invoice_date) AS first_sold_date,
    MAX(invoice_date) AS last_sold_date,
    unit_price,
    COUNT(DISTINCT invoice_no) AS total_orders,
    COUNT(DISTINCT CASE WHEN customer_id != 'Not Registered' THEN customer_id END)AS total_registered_customers,
    COUNT(DISTINCT CASE WHEN customer_id = 'Not Registered' THEN invoice_no END) AS total_guest_customers,
    SUM(quantity) AS total_quantity_sold
FROM {{ ref('stg_online_retail') }}
GROUP BY
    stock_code,
    description,
    unit_price