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
        """Read the CSV file and return rows as a list of dicts."""
        file_path = "/opt/airflow/data/superstore_sales.csv"
        rows = []
        with open(file_path, newline="", encoding="latin-1") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(dict(row))
        print(f"Extracted {len(rows)} rows.")
        return rows

    extract()

