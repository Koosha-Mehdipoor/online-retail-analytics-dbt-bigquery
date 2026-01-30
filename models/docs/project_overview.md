{% docs __overview__ %}

# Online Retail Analytics (dbt + BigQuery)

This project models online retail transactional data using dbt and BigQuery, with a
clear separation between ingestion, standardization, and analytics logic. The goal is
to provide a reliable, well-documented analytical foundation for sales, customer, and
product analysis.

---

## Architecture

This project follows a layered dbt modeling approach that separates ingestion,
standardization, and analytics responsibilities.

### Raw ingestion (`src_online_retail`)
Append-only ingestion of transactional records from the upstream source with minimal
transformation. This layer preserves the original structure and values of the data and
acts as a system-of-record for downstream models.

### Source views (`v_src_online_retail`)
Light standardization and normalization applied on top of the raw ingestion layer.
This includes trimming text fields, parsing timestamps, deriving basic status flags
(e.g. order status), and applying default labels for missing values.

### Staging (`stg_online_retail`)
Typed, deduplicated, and consistently structured records. Surrogate keys and derived
fields (such as date identifiers) are introduced here, providing a clean and stable
foundation for analytical models.

### Analytics layer (facts and dimensions)
Business-facing fact and dimension tables designed for analytics and reporting:
- Fact tables capture transactional measures such as sales, returns, and net amounts.
- Dimension tables provide descriptive context for customers, products, and dates.

---

## Key Modeling Decisions

- Guest customers are handled explicitly using a labeled customer identifier
  (e.g. `Not Registered`) to avoid data loss.
- Returns and cancellations are modeled using negative quantities and explicit
  return metrics.
- Customer geography is preserved at the transaction level and also derived at the
  customer level (e.g. most frequent country, last observed country).
- A surrogate date key (`invoice_date_id`) is used to support star-schema joins
  with the date dimension.

---

## Grain

- **Primary fact table grain**: one row per invoice line  
  (`invoice_no × stock_code`)

---

## Tooling

- dbt (core)
- BigQuery
- Incremental models with MERGE strategy
- dbt tests and documentation for data quality and transparency

{% enddocs %}
