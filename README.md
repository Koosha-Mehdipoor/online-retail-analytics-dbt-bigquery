# 📊 Online Retail Analytics – dbt + BigQuery + Power BI

End-to-end analytics project modeling transactional online retail data using:

- **dbt** for data transformation
- **BigQuery** as the data warehouse
- **Power BI** for reporting and visualization

This project demonstrates a layered data modeling approach, incremental pipelines,
star-schema design, and business-oriented analytics.

---

## 🏗 Architecture Overview

The project follows a layered modeling structure:

### 1️⃣ Raw Ingestion (`src_online_retail`)
- Incremental ingestion from upstream source
- Append-only structure
- Partitioned by ingestion timestamp
- Minimal transformation (system-of-record layer)

### 2️⃣ Source Standardization (`v_src_online_retail`)
- Trimmed text fields
- Parsed timestamps
- Derived `order_status`
- Labeled missing values (e.g., `Not Registered`, `Unknown`)

### 3️⃣ Staging (`stg_online_retail`)
- Typed fields
- Row-level hashing for deduplication
- Incremental MERGE strategy
- Surrogate key generation (`invoice_date_id`)
- Clean, analytics-ready structure

### 4️⃣ Analytics Layer

#### 📌 Fact Table
`f_online_retail`
- Grain: **invoice line (invoice_no × stock_code)**
- Measures:
  - `sales_amount`
  - `return_amount`
  - `net_amount`
- Preserves transaction-level country

#### 📌 Dimensions
- `dim_customers`
- `dim_products`
- `dim_dates`

---

## 🔑 Key Modeling Decisions

- Returns modeled using negative quantities
- Guest customers explicitly labeled
- Surrogate date keys for star-schema joins
- Incremental models with MERGE strategy
- Data quality tests applied at staging and mart layers

---

## 🧪 Data Quality

The project includes:
- `not_null` and `unique` tests
- Business-rule validation (e.g., order status logic)
- Relationship testing between fact and dimension tables

---

## 📈 Power BI Dashboard

The modeled data is consumed in Power BI to create:

- Revenue analysis
- Customer segmentation (RFM logic)
- Product performance
- Sales trends over time
- Return analysis

### Dashboard Preview

(See screenshots below)

---

## 📊 Example Dashboard Screenshots


![Dashboard Overview]<img width="874" height="497" alt="Dashboard Overview" src="https://github.com/user-attachments/assets/136d0250-c5b1-47fb-a0f8-a80f1912b49d" />
![Customer Analysis](images/customer_analysis.png)
![Product Performance](images/product_performance.png)

---

## 🗺 Data Lineage

dbt automatically generates documentation and lineage graphs:

To generate locally:

