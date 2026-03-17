# Financial Portfolio Data Pipeline & Reconciliation System

A data engineering project that builds an end-to-end pipeline for ingesting, validating, and reconciling financial portfolio data. The system simulates how investment platforms and family offices process portfolio transactions to ensure data integrity and produce analytics-ready datasets.

This project demonstrates **financial data engineering, ETL pipelines, reconciliation logic, and data quality monitoring** using Python, SQL, and a database backend (PostgreSQL locally, **SQLite for Streamlit Cloud**).

---

# Project Overview

Financial institutions process large volumes of portfolio transactions daily. Data inconsistencies between transactions, portfolio positions, and account balances can lead to inaccurate reporting and analytics.

This project builds a **data pipeline that ingests financial transaction data, validates data quality, reconciles portfolio balances, and produces clean datasets for downstream analytics and machine learning workflows.**

The system simulates a simplified version of the infrastructure used by **FinTech platforms, family offices, and asset management systems.**

---

# Key Features

• Built ETL pipelines to ingest financial transaction data using **Python and pandas**

• Designed a **PostgreSQL data warehouse** to store portfolio transactions, positions, and account balances

• Implemented **data validation checks** to detect anomalies such as invalid prices or negative quantities

• Developed **portfolio reconciliation logic** to compare computed positions from transactions against reported balances

• Generated **analytics-ready datasets** for financial reporting and machine learning pipelines

• Added **pipeline logging and data quality monitoring** to ensure reliability of financial datasets

---

# System Architecture

```
Raw Financial Data
    │
    │ CSV / API / Synthetic Data
    ▼
Ingestion Layer
Python + Pandas ETL
    │
    ▼
Database Warehouse
Transactions / Positions / Balances
    │
    ▼
Transformation Layer
SQL Models
    │
    ▼
Validation & Reconciliation Engine
Python + SQL
    │
    ▼
Analytics Ready Tables
portfolio_positions
transactions_clean
reconciliation_report
```

---

# Project Structure

```
financial-portfolio-data-pipeline/

data/
   raw/
   processed/

src/
   ingestion/
       generate_fake_data.py
       ingest_transactions.py

   validation/
       data_quality_checks.py

   reconciliation/
       reconcile_portfolio.py

sql/
   creates_tables.sql
   transformations.sql

notebooks/
   portfolio_analysis.ipynb

config/
   db_config.py

docker-compose.yml
requirements.txt
README.md
```

---

# Data Model

## Transactions

| column           | description                   |
| ---------------- | ----------------------------- |
| transaction_id   | unique transaction identifier |
| account_id       | investment account            |
| asset_symbol     | traded asset                  |
| transaction_type | BUY or SELL                   |
| quantity         | number of shares              |
| price            | asset price                   |
| transaction_date | timestamp                     |

---

## Portfolio Positions

| column         | description           |
| -------------- | --------------------- |
| account_id     | investment account    |
| asset_symbol   | asset ticker          |
| quantity       | number of shares held |
| market_value   | value of holdings     |
| valuation_date | date of valuation     |

---

## Cash Balances

| column       | description        |
| ------------ | ------------------ |
| account_id   | investment account |
| cash_balance | available cash     |
| balance_date | balance timestamp  |

---

# ETL Pipeline

The ETL pipeline performs three main stages.

### 1. Data Ingestion

Financial transaction datasets are ingested from raw CSV files into the PostgreSQL data warehouse using Python and pandas.

### 2. Data Validation

Data quality checks are applied to detect invalid records such as:

• negative transaction quantities
• invalid asset prices
• missing account identifiers

These checks ensure financial datasets remain reliable for downstream processing.

### 3. Portfolio Reconciliation

Portfolio positions are computed from transaction histories and compared against reported positions.

```
Net Position = Sum(BUY quantities) - Sum(SELL quantities)
```

Any discrepancies between computed and reported balances are flagged in a **reconciliation report**.

---

# Technologies Used

Python
Pandas
PostgreSQL
SQLite (Streamlit Cloud demo backend)
SQLAlchemy
NumPy
Docker
Faker (synthetic financial data generation)

---

# Setup Instructions

## Option A (recommended): Streamlit Community Cloud (no Docker)

This repo is Cloud-friendly by default. It uses a **file-backed SQLite database** at `data/pipeline.db`.

### Streamlit Cloud dependency note

Streamlit Cloud may default to a newer Python version. This repo includes `runtime.txt` to pin the Cloud runtime to **Python 3.12** for maximum compatibility.

This project also pins a modern Streamlit release (see `requirements.txt`) and constrains Altair to a compatible major version.

1) Install dependencies:

```
pip install -r requirements.txt
```

2) Run the pipeline end-to-end (generates fresh `data/raw/*.csv`, builds `data/pipeline.db`, and writes `reports/*.csv`):

```
DB_BACKEND=sqlite python -m src.main
```

3) Start the dashboard:

```
DB_BACKEND=sqlite streamlit run app.py
```

On Streamlit Cloud:
- set `DB_BACKEND=sqlite` (or rely on the default)\n+- use `app.py` as the entrypoint\n+- if you update `requirements.txt`, restart the app so dependencies reinstall cleanly

## Option B: Local Docker Postgres (optional)

### 1. Clone Repository

```
git clone https://github.com/yourusername/financial-portfolio-data-pipeline
cd financial-portfolio-data-pipeline
```

### 2. Install Dependencies

```
pip install -r requirements.txt
```

### 3. Start PostgreSQL

You can run PostgreSQL locally or using Docker.

```
docker compose up -d
```

### 4. Create Database Tables

```
make tables
```

### 5. Run End-to-End Pipeline

```
python -m src.main
```

### 6. Launch the Dashboard

```
streamlit run app.py
```

---

# Example Outputs

The pipeline generates the following datasets.

• **transactions_clean** – validated financial transactions
• **portfolio_positions** – computed asset positions
• **reconciliation_report** – discrepancies between reported and computed balances
• **data_quality_report** – validation errors and anomalies

---

# Future Improvements

Potential enhancements to extend the platform.

• Airflow orchestration for scheduled pipelines
• Streaming ingestion with Kafka
• dbt transformation layer for financial data modeling
• Great Expectations for advanced data quality monitoring
• Machine learning models for portfolio risk analysis
• LLM-powered financial data assistant

---

# Author

**Sajan Singh Shergill**

Master’s in Data Science
Pace University – Seidenberg School of Computer Science

GitHub Portfolio:
https://github.com/sajansshergill
