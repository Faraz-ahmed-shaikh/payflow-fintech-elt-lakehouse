# 💳 PayFlow Fintech ELT Lakehouse Pipeline

An end-to-end **ELT Lakehouse Project** built for a fictional digital payments & UPI company **PayFlow**, designed to simulate how raw fintech transaction data is transformed into **analytics-ready datasets** using **PySpark, Databricks, Delta Lake, and Medallion Architecture**.

The project demonstrates how modern data systems ingest, clean, transform, enrich, and model payment transaction data into business-ready marts for analysts and BI tools.

---

# 📌 Project Goal

The goal of this project was to build a **realistic Version 1 ELT pipeline** that reflects industry-relevant data engineering workflows without unnecessary enterprise complexity.

This project focuses on:

* API-style data ingestion
* PySpark transformations
* Databricks Lakehouse
* Delta Lake fundamentals
* Bronze → Silver → Gold Medallion Architecture
* Data cleaning & feature engineering
* Analytics marts & KPI generation
* Workflow orchestration using Databricks

---

# 🏢 Business Scenario

**PayFlow** is a fictional digital payments company handling large-scale UPI and digital transactions across India.

The business receives raw payment data containing:

* Duplicate transactions
* Missing values
* Timestamp inconsistencies
* Payment method variations
* Currency inconsistencies
* Merchant metadata issues

The objective is to transform unreliable raw data into:

> **trusted, analytics-ready datasets for Analysts, BI dashboards, and business reporting**

---

# 📊 Dataset Overview

### Transactions Dataset

* **301,500 total rows**
* **18,000 unique customers**
* **2,200+ merchants**
* Date range:
  **May 2024 → May 2026**

Includes:

* transaction_id
* customer_id
* merchant_id
* amount
* payment_method
* transaction_status
* transaction_timestamp
* city
* currency
* device_type

### Merchant Dataset

* **2,211 rows**
* Merchant metadata including:

  * merchant_name
  * merchant_category
  * merchant_tier
  * merchant_city

---

# ⚠️ Simulated Data Quality Issues

To mimic realistic fintech data challenges, synthetic issues were intentionally included:

### Transactions

* Missing merchant IDs
* Missing cities & device types
* Duplicate transaction rows
* Mixed timestamp formats
* Payment method inconsistencies
* Currency inconsistencies
* Null values

### Merchants

* Duplicate merchant records
* Missing merchant categories
* Category casing inconsistencies

---

# 🏗️ Architecture

![Architecture Diagram](architecture/payflow_architecture.png)

---

# 🔄 Medallion Architecture

## 🥉 Bronze Layer (Raw Data)

Raw JSONL datasets are ingested from GitHub using **Python Requests API ingestion** and stored in **Delta format**.

### Bronze Objectives

* Preserve raw data
* Add ingestion metadata
* Maintain data lineage

### Metadata Added

* `data_source`
* `layer`
* `ingested_at`

---

## 🥈 Silver Layer (Clean & Trusted Data)

The Silver layer standardizes and cleans raw datasets into trusted datasets.

### Transactions Cleaning

* Column renaming
* Duplicate removal
* Timestamp standardization
* Payment method standardization
* Currency normalization
* Missing value handling
* Data validation checks

### Merchant Cleaning

* Duplicate removal
* Category standardization
* Missing value handling
* Merchant metadata cleanup

---

## 🥇 Gold Layer (Analytics Ready)

Silver tables are enriched into analytics-ready datasets.

### Feature Engineering

#### Time Features

* transaction_date
* transaction_hour
* transaction_day_name
* transaction_month
* transaction_quarter
* transaction_year
* transaction_week_of_year
* is_weekend

#### Behavioral Features

* time_segment
* amount_bucket
* peak_hour_flag

#### Merchant Enrichment

Transaction data is enriched using merchant metadata through joins.

---

# 📈 Analytics Marts

The following marts were created for downstream analytics and BI consumption.

### 1. Merchant Performance Mart

Business metrics:

* Total GMV
* Success rate
* Average transaction value
* Total transactions
* Unique customers
* Peak transaction hour

---

### 2. Customer Spending Behavior Mart

Behavior metrics:

* Total spend
* Transaction frequency
* Favorite payment method
* Preferred transaction time
* Weekend transaction ratio
* Category preference

---

### 3. Payment Behavior Mart

Analysis of:

* Payment trends
* Payment success/failure
* Time-based payment behavior

---

### 4. Transaction Trend Mart

Trend metrics:

* Daily GMV
* Transaction volume
* Success rate
* Peak-hour activity

---

### 5. Executive KPI Table

High-level business KPIs including:

* Total Transactions
* Total GMV
* Success Rate
* Active Customers
* Active Merchants

---

# ⚙️ Workflow Orchestration

Pipeline orchestration was implemented using **Databricks Workflows**.

Execution Flow:

```text
01 Bronze Ingestion
        ↓
02 Bronze to Silver Transformation
        ↓
03 Silver to Gold Transformation
        ↓
04 Gold Marts & KPI Generation
```

### Workflow Screenshot

![Databricks Workflow](architecture/databricks_workflow.png)

---

# 🛠️ Tech Stack

### Languages & Libraries

* Python
* PySpark
* SQL

### Data Engineering Tools

* Databricks
* Delta Lake
* Databricks Workflows

### Architecture

* Medallion Architecture
* ELT Pipeline
* Lakehouse Architecture

### Storage Format

* JSONL
* Delta Tables

---

# 📂 Project Structure

```text
payflow-fintech-elt-lakehouse/

│── notebooks/
│   ├── 01_ingest_transactions_to_bronze.ipynb
│   ├── 02_bronze_to_silver_transformation.ipynb
│   ├── 03_silver_to_gold_transformation.ipynb
│   └── 04_gold_to_marts_tables.ipynb
│
│── source_files/
│   ├── 01_ingest_transactions_to_bronze.py
│   ├── 02_bronze_to_silver_transformation.py
│   ├── 03_silver_to_gold_transformation.py
│   └── 04_gold_to_marts_tables.py
│
│── data/
│   ├── transactions.json
│   └── merchant_metadata.json
│
│── architecture/
│   ├── payflow_architecture.png
│   └── databricks_workflow.png
│
└── README.md
```

---

# ❗ Notebook Loading Issue?

GitHub notebooks sometimes take time to load or may fail to render.

If notebooks are not opening:

👉 Please open the **`source_files/`** folder where all notebooks are also provided in **`.py` source format** for faster viewing.

---

### Workflow Configuration

The ELT pipeline was orchestrated using **Databricks Workflows** with dependency-based execution.

Task execution order:
1. **Bronze Ingestion**
   Fetch raw transaction and merchant datasets from GitHub and store them in Delta format.
2. **Bronze → Silver Transformation**
   Clean, standardize, validate, and prepare trusted datasets.
3. **Silver → Gold Transformation**
   Enrich transactions with merchant metadata and generate analytics-ready features.
4. **Gold → Analytics Marts & KPI Generation**
   Create business marts and executive KPI tables for downstream analysis.

Pipeline execution follows a sequential dependency chain to ensure downstream layers only run after upstream stages complete successfully.


---

# 🚀 Skills Demonstrated

* PySpark Transformations
* Databricks Lakehouse
* Delta Lake
* ELT Pipelines
* Medallion Architecture
* API Ingestion
* Data Cleaning
* Feature Engineering
* Analytics Engineering
* Workflow Orchestration
* KPI Modeling
* Business Analytics

---

## ⭐ If you found this project useful, feel free to star the repository.
