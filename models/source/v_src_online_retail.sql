
{{config(
    materialized = "view",
    tags = ["source"]
)}}

SELECT 
  trim(invoice_no) AS invoice_no,
  trim(stock_code) AS stock_code,
  coalesce(trim(description),"Not Defined") AS description,
  coalesce(trim(description),"Not Defined") || " - " || trim(stock_code) AS product_display,
  CASE WHEN left(trim(invoice_no),1) = 'C' AND stock_code <> 'D' THEN "cancelled" WHEN left(trim(invoice_no),1) = 'C' AND stock_code = 'D' THEN "discounted" ELSE "active" END AS order_status,
  quantity,
  EXTRACT (DATE FROM TIMESTAMP(safe.parse_datetime('%m/%d/%y %H:%M', trim(invoice_date)))) AS invoice_date,
  EXTRACT (TIME FROM TIMESTAMP(safe.parse_datetime('%m/%d/%y %H:%M', trim(invoice_date)))) AS invoice_time,
  unit_price,
  coalesce(customer_id, "Not Registered") AS customer_id,
  coalesce(country, "Unknown") AS country,
  created_at
FROM {{ref("src_online_retail")}}


