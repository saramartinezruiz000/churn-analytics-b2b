# B2B Churn Analytics & Automated ETL Pipeline

![Data Engineering & BI](https://img.shields.io/badge/Data_Engineering-Python_|_Pandas-blue.svg) ![PowerBI](https://img.shields.io/badge/Business_Intelligence-Power_BI_|_DAX-yellow.svg)

## 📌 Business Overview
In the B2B SaaS and Telecom industries, Customer Churn is one of the most critical metrics. This project addresses a scenario where a SaaS company ("TelcoConnect") experienced a 15% churn rate and lacked executive visibility due to data fragmentation.

This repository contains an **End-to-End Data Solution** that automates the extraction, cleaning, and modeling of disjointed corporate data (Customers, Subscriptions, Support Tickets) into a robust **Data Warehouse** (SQLite/PostgreSQL proxy) analyzed via **Power BI**.

## 🛠️ Architecture & Tech Stack
1. **Data Generation / Mocking**: Python script (`data_generator.py`) generates highly realistic, "dirty" synthetic B2B data (nulls, format inconsistencies, anomalies).
2. **Automated ETL Pipeline**: Python (`etl_pipeline.py`) utilizing `pandas` for aggressive data cleaning and standardization. Implements robust exception handling and operational logging.
3. **Data Warehouse (Star Schema)**: Data is modeled into a Star Schema (`dim_customers`, `fact_subscriptions`, `fact_tickets`) using SQL.
4. **Business Intelligence**: Advanced DAX Measures (Time Intelligence, Cohorts) are documented and prepped for a Power BI Dashboard.

## 📁 Repository Structure
```text
├── data/
│   ├── raw/                 # Raw, "dirty" CSV files
│   └── processed/           # Cleaned SQLite Data Warehouse (churn_analytics_dw.db)
├── logs/                    # Automated execution logs (etl_pipeline.log)
├── data_generator.py        # Python script: Synthetic data generation
├── etl_pipeline.py          # Python script: End-to-End ETL processing
├── schema.sql               # SQL: Relational tables & analytical views
└── DAX_Measures.md          # DAX: Business logic and formulas for Power BI
```

## 🚀 How to Run Locally

1. **Clone the repository:**
   ```bash
   git clone https://github.com/saramartinezruiz000/churn-analytics-b2b.git
   cd churn-analytics-b2b
   ```

2. **Generate the raw data (simulating legacy systems):**
   ```bash
   python data_generator.py
   ```

3. **Run the Automated ETL Pipeline:**
   ```bash
   python etl_pipeline.py
   ```
   *You can verify the cleaning process in `logs/etl_pipeline.log` and inspect the normalized Data Warehouse at `data/processed/churn_analytics_dw.db`.*

## 📈 Key Impact Returns (ROI)
- **Operational Efficiency**: Transformed 10+ hours a week of manual CSV cleaning into a 5-second automated pipeline.
- **Data Quality**: Implemented median-imputations and data standardization rules protecting downstream BI reports.
- **Strategic Visibility**: Centralized fragmented Subscription & Ticketing data into a single analytical view `vw_churn_analysis` for the C-Suite.
