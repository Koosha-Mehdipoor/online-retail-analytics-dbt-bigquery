{{ 
  config(
    materialized='incremental',
    tags = ["source"],
    on_schema_change='ignore',
    incremental_strategy='merge',
    unique_key='row_hash',
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
  to_hex(
    sha256(
      to_json_string(
        struct(
          trim(cast(InvoiceNo AS string))   AS InvoiceNo,
          trim(cast(StockCode AS string))   AS StockCode,
          trim(cast(Description AS string)) AS Description,
          trim(cast(Quantity AS string))    AS Quantity,
          trim(cast(InvoiceDate AS string)) AS InvoiceDate,
          trim(cast(UnitPrice AS string))   AS UnitPrice,
          trim(cast(CustomerID AS string))  AS CustomerID,
          trim(cast(Country AS string))     AS Country
        )
      )
    )
  ) AS row_hash,

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
    AND invoice_date BETWEEN TIMESTAMP('{{ var("start_date") }}')
                         AND TIMESTAMP('{{ var("end_date") }}')
  {% else %}
    {{ log(
      'Loading ' ~ this ~ ' incrementally (based on max invoice_date in target table)',
      info=true
    ) }}
    AND invoice_date > (
      SELECT COALESCE(MAX(invoice_date), TIMESTAMP('1900-01-01'))
      FROM {{ this }}
    )
  {% endif %}
{% endif %}
