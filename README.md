# 📊 Online Retail Analytics – dbt + BigQuery + Power BI

End-to-end analytics project modeling transactional online retail data using:

- **dbt** for data transformation
- **BigQuery** as the data warehouse
- **Power BI** for reporting and visualization

This project demonstrates a layered data modeling approach, incremental pipelines,
star-schema design, and business-oriented analytics.

---

## 📂 Dataset

Source: UCI Machine Learning Repository  
Online Retail Data Set  
https://archive.ics.uci.edu/ml/datasets/Online+Retail

This dataset contains transactional data for a UK-based online retail company
covering the period 01/12/2010 to 09/12/2011.

The data includes invoice numbers, product identifiers, quantities, prices,
customer IDs, and country information, making it suitable for transactional,
dimensional, and historical modeling use cases.


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

## 🔄 Slowly Changing Dimension (SCD Type 2)

This project implements **Slowly Changing Dimension (SCD) Type 2** logic using
dbt Snapshots to track historical changes in product pricing.

### 📌 Use Case: Product Canonical Price Tracking

Product prices may change over time. Instead of overwriting previous values,
the project versions product price records to preserve historical accuracy.

When the `canonical_price` changes:

- The existing record is closed (`dbt_valid_to` is populated)
- A new versioned record is created
- Validity periods are automatically maintained by dbt

This enables:

- Historical price analysis  
- Time-aware revenue reporting  
- Accurate backdated financial analysis  
- Tracking product price evolution  

---

### 🛠 Implementation Highlights

- Snapshot strategy: `check`
- Unique key: `stock_code`
- Tracked column: `canonical_price`
- Snapshot-managed fields:
  - `dbt_valid_from`
  - `dbt_valid_to`
  - `dbt_scd_id`

Fact tables can be joined to the snapshot using validity ranges to ensure
time-accurate reporting based on historical price conditions.

---

### 🚀 Why This Matters

Implementing SCD Type 2 demonstrates:

- Advanced dimensional modeling
- Historical data versioning
- Production-ready warehouse design
- Real-world pricing change management

This elevates the project beyond basic analytics into data engineering best practices.


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

### Please Note that since the data is for 2011 the RFM analysis is not showing the anaysis perfectly but in the real world data would be much more meaningful.

Executive Dashboard:

<img width="874" height="497" alt="Dashboard Overview" src="https://github.com/user-attachments/assets/136d0250-c5b1-47fb-a0f8-a80f1912b49d" />

Product Analysis:

<img width="878" height="493" alt="Screenshot 2026-02-18 at 11 45 17" src="https://github.com/user-attachments/assets/649b72c5-8903-44ae-9ba2-f43bc5981a23" />

Customer Analysis:

<img width="875" height="492" alt="Screenshot 2026-02-18 at 11 45 36" src="https://github.com/user-attachments/assets/81a0f93a-3cff-4644-9ed6-e7467b0e83fe" />



---

## 🗺 Data Lineage

dbt automatically generates documentation and lineage graphs:

To generate locally:

dbt docs generate
dbt docs serve

Project Overview:

<img width="1460" height="834" alt="Screenshot 2026-02-18 at 11 58 24" src="https://github.com/user-attachments/assets/fd47e745-fd57-47bc-b4e3-cb4bdee6827e" />


Models and their detail overview:


<img width="1465" height="831" alt="Screenshot 2026-02-18 at 11 58 53" src="https://github.com/user-attachments/assets/ca6bfb52-9162-4742-8e9e-83a8a920e3a6" />



Linage:


<img width="1432" height="789" alt="Screenshot 2026-02-18 at 11 59 36" src="https://github.com/user-attachments/assets/378d7de6-bcc3-4396-992c-ade57cb04cb8" />




---

## 🛠 Tech Stack

- dbt Core
- BigQuery
- dbt-utils
- Power BI
- SQL

---

## 🚀 What This Project Demonstrates

- Incremental ELT pipeline design
- Clean star-schema modeling
- Handling real-world messy data
- Dimensional modeling best practices
- Business-driven metric design
- Documentation and lineage management

---

## 📄 License

This project is licensed under the MIT License.

You are free to use, modify, and distribute this project for educational and
professional purposes, provided that proper attribution is given.

See the `LICENSE` file for full license details.


## 👤 Author

Koosha  
Data Analytics & Engineering Portfolio Project

