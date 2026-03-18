# 📦 FulfillNow Data Engineering Pipeline (Snowflake)

## 📌 Overview
This project implements an end-to-end ELT data pipeline in Snowflake using a Bronze–Silver–Gold architecture for fulfillment and logistics analytics.

It processes data from:
- Customers
- Orders
- Employees
- Inventory
- Events

---

## 🏗️ Architecture
Raw Data → Bronze → Silver → Gold → KPIs / Analytics

### 🔹 Bronze Layer
- Raw data ingestion
- No transformations
- Data loaded from internal stage (CSV)

### 🔹 Silver Layer
- Data cleaning & validation
- Deduplication using ROW_NUMBER()
- Business rules enforcement
- Referential integrity

### 🔹 Gold Layer
- KPI views
- Analytical views
- SCD Type-2 dimensions
- Security (Masking & RLS)

---

## ⚙️ Tech Stack
- Snowflake (SQL)
- Streams & Tasks (Automation)
- ELT Pipeline
- CSV Data Files
- Tableau (optional)

---

## 🚀 Setup Instructions

### 1. Create Warehouse
```sql
CREATE OR REPLACE WAREHOUSE bronze_wh
WAREHOUSE_SIZE = 'XSMALL'
AUTO_SUSPEND = 60
AUTO_RESUME = TRUE;
```

### 2. Create Database & Schemas
```sql
CREATE DATABASE fulfillnow_db;

CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
```

### 3. Create File Format & Stage
```sql
CREATE FILE FORMAT csv_format
TYPE = CSV
SKIP_HEADER = 1
DATE_FORMAT = 'DD-MM-YYYY';

CREATE STAGE bronze_stage
FILE_FORMAT = csv_format;
```

### 4. Load Data
```sql
COPY INTO raw_tables FROM @bronze_stage;
```

---

## 📂 Data Pipeline

### 🔹 Bronze Tables
- raw_customers
- raw_orders
- raw_employees
- raw_inventory
- raw_events

### 🔹 Silver Tables
- customers_clean
- orders_clean
- employees_clean
- inventory_clean
- events_clean
- orders_valid
- events_final

✔ Transformations:
- Deduplication
- Data validation
- Referential joins

---

## 🔹 Gold Layer

### 📊 KPI Views
- kpi_sla_score
- kpi_throughput_rate
- kpi_employee_productivity
- kpi_inventory_availability
- kpi_anomaly_rate

### 📈 Analytical Views
- SLA Status
- Event Sequence
- Missing Shipments
- Event Count

---

## 🔄 SCD Type-2

### Tables
- dim_customers
- dim_employees

### Features
- Tracks historical changes
- Columns:
  - start_date
  - end_date
  - current_flag

---

## ⚡ Automation

### Streams (CDC)
- customers_stream
- employees_stream
- orders_stream
- events_stream

### Tasks
- anomaly_task (runs every 10 minutes)
- scd_customer_task (auto updates)

---

## 🚨 Data Quality

### Anomaly Detection
Detects:
- SLA breaches
- Inventory mismatch
- Invalid event sequences

```sql
CALL detect_anomalies();
```

### Exception Logging
- Missing customers
- Invalid timelines

---

## 🔐 Security

### Data Masking
- Email masking for non-admin users

### Row-Level Security
- Access based on hub_id

---

## 📊 KPIs

| KPI | Description |
|-----|------------|
| SLA Score | % orders shipped on time |
| Throughput Rate | Events processed per hour |
| Employee Productivity | Events per employee |
| Inventory Availability | Stock availability |
| Anomaly Rate | % problematic orders |

---

## 📈 Sample Queries
```sql
SELECT * FROM gold.kpi_sla_score;
SELECT * FROM gold.kpi_employee_productivity;
SELECT * FROM gold.vw_sla_status;
```

---

## 📁 Project Script
See full SQL implementation in:
project.sql

---

## 🎯 Use Cases
- Logistics Analytics
- Warehouse Monitoring
- SLA Tracking
- Inventory Optimization
- Employee Performance Analysis

---

## ⭐ Key Highlights
- End-to-End ELT Pipeline
- Bronze–Silver–Gold Architecture
- SCD Type-2 Implementation
- Streams & Tasks Automation
- Data Quality Checks
- Security (Masking + RLS)
- KPI-driven insights
