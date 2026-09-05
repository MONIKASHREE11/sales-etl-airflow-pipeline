import csv
import psycopg2
from datetime import datetime, timedelta
from airflow.sdk import DAG, task

default_args = {
    "owner": "monika",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="sales_etl_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "sales"],
    description="Daily ETL pipeline: extracts Superstore sales data, enriches with INR conversion, and loads into PostgreSQL.",
) as dag:

    @task()
    def extract():
        file_path = "/opt/airflow/data/superstore_sales.csv"
        rows = []
        with open(file_path, newline="", encoding="latin-1") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(dict(row))
        if not rows:
            raise ValueError("No data found in CSV file!")
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
        if not cleaned:
            raise ValueError("No rows survived transformation!")
        print(f"Transformed {len(cleaned)} rows.")
        return cleaned

    @task()
    def load(rows):
        conn = psycopg2.connect(
            host="etl_postgres",
            port=5432,
            dbname="sales_data",
            user="etl_user",
            password="etl_password"
        )
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE sales;")
        insert_query = """
            INSERT INTO sales (order_id, order_date, category, sub_category,
                               product_name, sales_usd, sales_inr, quantity, region)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        for row in rows:
            cursor.execute(insert_query, (
                row["order_id"], row["order_date"], row["category"],
                row["sub_category"], row["product_name"], row["sales_usd"],
                row["sales_inr"], row["quantity"], row["region"]
            ))
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Loaded {len(rows)} rows into PostgreSQL.")

    extracted = extract()
    enriched = enrich(extracted)
    transformed = transform(enriched)
    load(transformed)