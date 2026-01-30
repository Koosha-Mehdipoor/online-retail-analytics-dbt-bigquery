{{
config(
    materialized='view',
    tags=['mart']
)
}}

SELECT f.*,
    d.date AS invoice_date,
    d.year,
    d.month,
    d.quarter,
    d.day_of_month,
    d.day_of_week,
    d.weekday_name,
    d.week_of_year,
    d.is_weekend
FROM {{ ref ('f_online_retail') }} AS f
JOIN {{ ref ('dim_dates') }} AS d
    ON f.invoice_date_id = d.date_id
WHERE d.date >= DATE_SUB((SELECT MAX(invoice_date) FROM {{ ref('stg_online_retail') }}), INTERVAL 1 YEAR)
