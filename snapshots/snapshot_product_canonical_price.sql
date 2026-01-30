{% snapshot snapshot_product_canonical_price %}

{{
  config(
    target_schema='snapshots_online_retail',
    unique_key='stock_code',
    strategy='check',
    check_cols=['canonical_price']
  )
}}

select
  stock_code,
  canonical_price
from {{ ref('product_canonical_price') }}

{% endsnapshot %}
