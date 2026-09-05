# 🚀 Sales Data ETL Pipeline

An automated ETL (Extract, Transform, Load) pipeline built with **Apache Airflow**, **PostgreSQL**, and **Docker** — designed as a Data Engineer portfolio project.

---

## 📌 Project Overview

This pipeline runs **daily** and automatically:
1. **Extracts** raw sales data from a CSV file (Kaggle Superstore dataset)
2. **Enriches** it with a USD → INR currency conversion
3. **Transforms** it by cleaning, formatting, and selecting key columns
4. **Loads** the cleaned data into a PostgreSQL database

---

## 🛠️ Tech Stack

| Tool | Purpose |
|---|---|
| Apache Airflow 3.2.2 | Pipeline orchestration & scheduling |
| PostgreSQL 16 | Data warehouse |
| Docker & Docker Compose | Containerised environment |
| Python 3.13 | ETL logic |
| psycopg2 | PostgreSQL connector |

---

## 🗂️ Project Structure

airflow-etl/
├── dags/
│ └── sales_etl_dag.py # Main ETL DAG
├── data/
│ └── superstore_sales.csv # Source dataset
├── docker-compose.yaml # 8-container Airflow setup
└── README.md


---

## ⚙️ Pipeline Architecture

CSV File → Extract → Enrich (USD→INR) → Transform → Load → PostgreSQL


---

## 📊 Dataset

- **Source:** Kaggle Superstore Sales Dataset
- **Size:** 9,994 rows
- **Key columns:** Order ID, Order Date, Category, Sales, Quantity, Region

---

## 🚀 How to Run

1. Clone the repo
2. Install Docker Desktop
3. Run:
```bash
docker compose up -d
```
4. Open Airflow UI at `http://localhost:8080`
5. Trigger the `sales_etl_pipeline` DAG

---

## ✅ Features

- Daily automated schedule
- Automatic retries (3 attempts) on failure
- Currency enrichment (USD → INR)
- Data cleaning and type formatting
- Loads into a real PostgreSQL database

---

## 👩‍💻 Author

**T. Monika Shree**
MSc Data Science — CHRIST (Deemed to be University), Bengaluru
[GitHub](https://github.com/MONIKASHREE11)