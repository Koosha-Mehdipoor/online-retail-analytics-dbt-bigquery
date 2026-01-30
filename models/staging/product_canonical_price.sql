{{
config(
    materialized='table',
    tags=['intermediary']
)
}}

WITH BASE AS(
SELECT 
    stock_code,
    CAST(unit_price AS NUMERIC) AS unit_price,
    invoice_date
FROM {{ ref('stg_online_retail') }}
WHERE unit_price IS NOT NULL AND unit_price >0
AND stock_code IS NOT NULL AND quantity > 0
AND invoice_date >= DATE_SUB((SELECT MAX(invoice_date) FROM {{ ref('stg_online_retail') }}), INTERVAL 6 MONTH))

, canonical AS (
    SELECT 
        stock_code,
        percentile_cont(unit_price,0.5) OVER(PARTITION BY stock_code) AS canonical_price
    FROM BASE
)

SELECT 
    DISTINCT
    stock_code,
    canonical_price
FROM canonical