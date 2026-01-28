{{
    config(
        materialized = 'table',
        cluster_by = 'customer_id',
        tags = ['dimensions']
    )
}}
WITH INVOICE_TOTALS AS (
    SELECT 
    customer_id,
    invoice_no,
    invoice_date,
    SUM(quantity * unit_price) AS order_value,
    SUM(quantity) AS order_quantity
    FROM {{ref("stg_online_retail")}}
    GROUP BY
    customer_id,invoice_no,invoice_date
),
BASE AS (
    SELECT 
    customer_id,
    MIN(invoice_date) AS first_order_date,
    MAX(invoice_date) AS last_order_date,
    SUM(order_value) AS total_value,
    COUNT(DISTINCT invoice_no) AS total_orders,
    SUM(order_quantity) AS total_quantity,
    ROUND(AVG(order_value),0) AS avg_order_value

FROM INVOICE_TOTALS
GROUP BY
    customer_id
)
SELECT 
* EXCEPT(most_frequent_country, last_country), mfc.most_frequent_country, mfc.last_country
FROM BASE
LEFT JOIN {{ref("eph_most_frequent_country")}} AS mfc
USING (customer_id)