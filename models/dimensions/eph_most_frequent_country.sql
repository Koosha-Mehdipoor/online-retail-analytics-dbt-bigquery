{{
    config(
        materialized = 'ephemeral',
        tags = ['dimensions']
    )
}}
-- this ephemeral model identifies the most frequent country for each customer, we have used array_agg instead row_number because it works better in bigquery since its columnar database
WITH COUNTRY_RANKED AS (
    SELECT
        customer_id,
        country,
        COUNT(*) AS cnt
    FROM {{ ref('stg_online_retail') }}
    WHERE customer_id IS NOT NULL
    GROUP BY customer_id, country
), 
PRIMARY_COUNTRY AS (
    SELECT  
        customer_id,
        ARRAY_AGG(country ORDER BY cnt DESC LIMIT 1)[OFFSET(0)] AS most_frequent_country
    FROM COUNTRY_RANKED
    GROUP BY customer_id
),
LAST_COUNTRY AS (
    SELECT
        customer_id,
        country AS last_country

        FROM {{ ref('stg_online_retail') }}
        WHERE customer_id IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY invoice_date DESC, invoice_no DESC) = 1
        )

SELECT 
    customer_id,
    most_frequent_country,
    LAST_COUNTRY.last_country AS last_country
FROM PRIMARY_COUNTRY
LEFT JOIN LAST_COUNTRY USING (customer_id)