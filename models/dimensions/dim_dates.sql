{{
  config(
    materialized = 'table',
    cluster_by = 'date_id',
    tags = ['dimensions']
    )
}}


WITH calendar AS (
  SELECT day
  FROM UNNEST(GENERATE_DATE_ARRAY('2010-11-01', '2012-12-31')) AS day
)
SELECT
  CAST(FORMAT_DATE('%Y%m%d', day) AS INT64) AS date_id,  
  day AS date,
  EXTRACT(YEAR FROM day) AS year,
  EXTRACT(QUARTER FROM day) AS quarter,
  EXTRACT(MONTH FROM day) AS month,
  EXTRACT(DAY FROM day) AS day_of_month,
  EXTRACT(DAYOFWEEK FROM day) AS day_of_week,
  FORMAT_DATE('%A',day) weekday_name,  
  EXTRACT(ISOWEEK FROM day) AS week_of_year,
  CASE WHEN EXTRACT(DAYOFWEEK FROM day) IN (1,7) THEN TRUE ELSE FALSE END AS is_weekend
FROM calendar