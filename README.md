# 💳 PayFlow Fintech ELT Lakehouse Pipeline

An end-to-end **ELT Lakehouse Project** built for a fictional digital payments & UPI company called **PayFlow**.

This project was built to understand how raw fintech transaction data moves through a modern data system using **PySpark, Databricks, Delta Lake, and Medallion Architecture**, while keeping things practical and industry-relevant for a beginner-to-intermediate level project.

The goal was simple:

> Build a system that takes messy payment transaction data, stores it efficiently in a centralized Lakehouse, and transforms it into trusted, analytics-ready datasets for Analysts and BI reporting.

---

# 🚀 Why I Built This

Most of my projects are analytics-focused, but I also wanted to understand:

* How raw data gets ingested
* How end-to-end data pipelines are built
* How data is stored and organized efficiently in modern storage systems
* How data is cleaned and standardized at scale
* How modern data teams structure datasets for analysts and business reporting


Instead of building something overengineered, I wanted a **realistic Version 1 Proof of Concept (POC)** similar to how an actual company might structure a basic fintech ELT workflow.

---

# 🏢 Business Scenario

**PayFlow** is a fictional digital payments company processing UPI and digital transactions across India.

The company receives raw transaction data with real-world messiness:

* Duplicate transactions
* Missing merchant IDs, cities & device types
* Mixed timestamp formats
* Payment method inconsistencies (`upi`, `UPI`, `CC`, etc.)
* Currency inconsistencies (`inr`, `INR ₹`, etc.)

The challenge:

> Convert unreliable raw data into clean, trusted, analytics-ready tables for business reporting.

---

# 📊 Dataset Overview

### Transactions Dataset

* **301,500 transaction records**
* **18,000 unique customers**
* **2,200+ merchants**
* Date Range: **May 2024 → May 2026**

### Merchant Dataset

* **2,211 merchant records**
* Merchant metadata such as:

  * merchant name
  * merchant category
  * merchant tier
  * merchant city

---

# 🏗️ Architecture

This project follows the **Medallion Architecture** approach:

### 🥉 Bronze Layer → Raw Data

Raw JSONL transaction and merchant datasets are ingested from GitHub using Python `requests` and stored as **Delta Tables**.

Metadata added:

* `data_source`
* `layer`
* `ingested_at`

---

### 🥈 Silver Layer → Clean & Trusted Data

Raw data is standardized and cleaned using **PySpark transformations**.

Key transformations:

**Transactions**

* Duplicate removal
* Timestamp parsing
* Payment method standardization
* Currency normalization
* Missing value handling
* Data quality validation

**Merchants**

* Duplicate cleanup
* Category standardization
* Null handling

---

### 🥇 Gold Layer → Analytics Ready

Clean datasets are enriched and transformed into business-ready tables.

Feature engineering includes:

* Transaction date & hour
* Weekend flag
* Time segment (Morning / Evening / Night)
* Amount buckets
* Peak-hour indicators
* Merchant enrichment

---

# 📈 Analytics Marts

To make the data useful for analysts and dashboards, I created business-focused marts:

### Merchant Performance Mart

Tracks:

* Transaction volume
* Success rate
* Gross Merchandise Value
* Unique customers
* Average transaction value

### Customer Spending Behavior Mart

Tracks:

* Spending patterns
* Favorite payment method
* Preferred transaction time
* Weekend behavior

### Payment Behavior Mart

Analyzes:

* Payment success/failure patterns
* Payment method trends
* Time-based payment behavior

### Transaction Trend Mart

Tracks:

* Daily transaction volume
* Daily payment value
* Success trends
* Peak-hour activity

### Executive KPI Table

High-level business metrics:

* Total Transactions
* Total Payment Value
* Success Rate
* Active Customers
* Active Merchants

---

# ⚙️ Workflow Orchestration

The full pipeline was orchestrated using **Databricks Workflows**.

Execution Flow:

```text
01 Bronze Ingestion
        ↓
02 Bronze → Silver Transformation
        ↓
03 Silver → Gold Transformation
        ↓
04 Gold → Analytics Marts & KPI Generation
```

Workflow Screenshot: available in architecture folder

---
# 📊 Business Dashboard (Sample Inshigts)
<img width="800" height="400" alt="dashboard_image" src="https://github.com/user-attachments/assets/5f276751-5fd7-4a99-8aca-c2c0a94b847c" />


# 🛠️ Tech Stack

**Languages & Processing**

* Python
* PySpark

**Data Engineering**

* Databricks
* Delta Lake
* ELT Pipelines
* Medallion Architecture

**Storage**

* JSONL
* Delta Tables

---

# 📂 Project Structure

```text
payflow-fintech-elt-lakehouse/

│── notebooks/
│── source_files/
|── marts_visualization/ 
│── data/
│── architecture/
└── README.md
```

> GitHub notebooks sometimes fail to render.
> If notebooks don't open, please check the **`source_files/`** folder where all notebooks are also available as `.py` files for faster viewing.

---

# ⚠️ Limitations

* This project uses **synthetic fintech data**, so real-world seasonality patterns may not fully exist.
* This is a **Version 1 Proof of Concept**, so advanced production features like streaming, CDC, incremental loading, rollback logic, and complex exception handling were intentionally kept out of scope.

---

# 🚀 Skills Demonstrated

* PySpark
* Databricks
* Delta Lake
* ELT Pipelines
* Medallion Architecture
* Data Cleaning
* Feature Engineering
* Analytics Engineering
* Workflow Orchestration
* KPI Modeling

⭐ If you found this project interesting, feel free to star the repository.
