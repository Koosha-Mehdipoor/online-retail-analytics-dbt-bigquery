{{ 
    config(
        materialized = 'table',
        cluster_by = 'invoice_date_id',
        tags = ['facts']
    )
}}

SELECT 
row_hash,
invoice_date_id,
invoice_no,
stock_code,
quantity,
order_status,
customer_id,
country,
CASE WHEN quantity > 0 THEN quantity * unit_price ELSE 0 END AS sales_amount,
CASE WHEN quantity < 0 THEN quantity * unit_price ELSE 0 END AS return_amount,
quantity*unit_price AS net_amount
FROM {{ ref('stg_online_retail') }}