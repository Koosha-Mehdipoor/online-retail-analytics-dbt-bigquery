{{ 
  config(
    materialized='incremental',
    tags = ["source"],
    on_schema_change='ignore',
    partition_by={
      "field": "created_at",
      "data_type": "timestamp",
      "granularity": "day"
    }
  ) 
}}

WITH raw_data AS (
    SELECT *
    FROM {{ source('online_retail', 'raw_data') }}
)

SELECT 
  InvoiceNo   AS invoice_no,
  StockCode   AS stock_code,
  Description AS description,
  Quantity    AS quantity,
  InvoiceDate AS invoice_date,
  UnitPrice   AS unit_price,
  CustomerID  AS customer_id,
  Country     AS country,
  CURRENT_TIMESTAMP() AS created_at
FROM raw_data
WHERE 1=1

{% if is_incremental() %}
  {% if var("start_date", none) and var("end_date", none) %}
    {{ log(
      'Loading ' ~ this ~ ' incrementally (start_date: ' ~ var("start_date") ~ ', end_date: ' ~ var("end_date") ~ ')',
      info=true
    ) }}
    AND InvoiceDate BETWEEN TIMESTAMP('{{ var("start_date") }}')
                         AND TIMESTAMP('{{ var("end_date") }}')
  {% else %}
    {{ log(
      'Loading ' ~ this ~ ' incrementally (based on max InvoiceDate in target table)',
      info=true
    ) }}
    AND CAST(InvoiceDate AS TIMESTAMP) > (
      SELECT COALESCE(MAX(CAST(InvoiceDate AS TIMESTAMP)), TIMESTAMP('1900-01-01'))
      FROM {{ this }}
    )
  {% endif %}
{% endif %}
