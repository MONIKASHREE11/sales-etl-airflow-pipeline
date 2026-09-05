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

    @task()
    def transform(rows):
        cleaned = []
        for row in rows:
            try:
                date_str = row.get("Order Date", "").strip()
                parsed_date = datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
                cleaned.append({
                    "order_id": row.get("Order ID", "").strip(),
                    "order_date": parsed_date,
                    "category": row.get("Category", "").strip(),
                    "sub_category": row.get("Sub-Category", "").strip(),
                    "product_name": row.get("Product Name", "").strip(),
                    "sales_usd": float(row.get("Sales", 0)),
                    "sales_inr": row.get("Sales_INR"),
                    "quantity": int(row.get("Quantity", 0)),
                    "region": row.get("Region", "").strip(),
                })
            except Exception as e:
                print(f"Skipping row due to error: {e}")
                continue
        print(f"Transformed {len(cleaned)} rows.")
        return cleaned

    extracted = extract()
    enriched = enrich(extracted)
    transform(enriched)