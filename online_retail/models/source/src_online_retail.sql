
{{config(
    materialized='incremental',
    on_schema_change='ignore',
    partition_by: {
        "field": "created_at",
        "data_type": "timestamp",
        "granularity": "day"
    },
    incremental_strategy= merge
)}}

WITH raw_data AS (
    SELECT * FROM {{source('online_retail', 'raw_data')}}
)

SELECT 
   to_hex(sha256(to_json_string(struct(
    trim(cast(InvoiceNo as string)) as InvoiceNo,
    trim(cast(StockCode as string)) as StockCode,
    trim(cast(Description as string)) as Description,
    trim(cast(Quantity as string)) as Quantity,
    trim(cast(InvoiceDate as string)) as InvoiceDate,
    trim(cast(UnitPrice as string)) as UnitPrice,
    trim(cast(CustomerID as string)) as CustomerID,
    trim(cast(Country as string)) as Country
  )))) as row_hash
 InvoiceNo AS invoice_no,
 StockeCode AS stockCode,
 Description AS description,
 Quantity AS quantity,
 InvoiceDate AS invoice_date,
 UnitPrice AS unit_price,
 CustomerID AS customer_id,
 Country AS country,
 current_timestamp() AS created_at

 FROM raw_data