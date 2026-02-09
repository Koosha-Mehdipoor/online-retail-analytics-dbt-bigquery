{{ 
  config(
    materialized='incremental',
    tags = ["staging"],
    on_schema_change='ignore',
    incremental_strategy='merge',
    unique_key='row_hash',
    cluster_by='invoice_date_id'

  ) 
}}
-- in the config our incremental model is defined as merge strategy and by providing a unique key it update the data if there is any change in the source data
WITH raw_data AS (
    SELECT *
    FROM {{ ref("v_src_online_retail") }}
)

SELECT 
  -- creating a unique hash id for each row in this case help us dedupliacte our table
  to_hex(
    sha256(
      to_json_string(
        struct(
          trim(cast(invoice_no AS string))   AS invoice_no,
          trim(cast(stock_code AS string))   AS stock_code,
          trim(cast(description AS string)) AS description,
          trim(cast(quantity AS string))    AS quantity,
          trim(cast(invoice_date AS string)) AS invoice_date,
          trim(cast(invoice_time AS string)) AS invoice_time,
          trim(cast(unit_price AS string))   AS unit_price,
          trim(cast(customer_id AS string))  AS customer_id,
          trim(cast(country AS string))     AS country
        )
      )
    )
  ) AS row_hash,
  CAST(FORMAT_DATE('%Y%m%d', invoice_date) AS INT64) AS invoice_date_id,
  invoice_no,
  stock_code,
  description,
  product_display,
  order_status,
  quantity,
  invoice_date,
  unit_price,
  customer_id,
  country,
  created_at
FROM raw_data
WHERE 1=1
QUALIFY ROW_NUMBER() OVER (PARTITION BY row_hash ORDER BY invoice_date_id DESC) = 1

{% if is_incremental() %}
  {% if var("start_date", none) and var("end_date", none) %}
    {{ log(
      'Loading ' ~ this ~ ' incrementally (start_date: ' ~ var("start_date") ~ ', end_date: ' ~ var("end_date") ~ ')',
      info=true
    ) }}
    AND invoice_date BETWEEN DATE('{{ var("start_date") }}')
                         AND DATE('{{ var("end_date") }}')
  {% else %}
    {{ log(
      'Loading ' ~ this ~ ' incrementally (based on max invoice_date in target table)',
      info=true
    ) }}
    AND invoice_date > (
      SELECT COALESCE(MAX(invoice_date), DATE('1900-01-01'))
      FROM {{ this }}
    )
  {% endif %}
{% endif %}
