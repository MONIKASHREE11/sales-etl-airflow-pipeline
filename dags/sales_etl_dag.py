import csv
from datetime import datetime
from airflow.sdk import DAG, task

with DAG(
    dag_id="sales_etl_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "sales"],
) as dag:

    @task()
    def extract():
        file_path = "/opt/airflow/data/superstore_sales.csv"
        rows = []
        with open(file_path, newline="", encoding="latin-1") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(dict(row))
        print(f"Extracted {len(rows)} rows.")
        return rows

    @task()
    def enrich(rows):
        rate = 83.95
        print(f"Exchange rate: 1 USD = {rate} INR")
        for row in rows:
            try:
                sales_usd = float(row["Sales"])
                row["Sales_INR"] = round(sales_usd * rate, 2)
            except (ValueError, KeyError):
                row["Sales_INR"] = None
        print(f"Enriched {len(rows)} rows.")
        return rows

    extracted = extract()
    enrich(extracted)