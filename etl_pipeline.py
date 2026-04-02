"""ETL Pipeline — Amman Digital Market Customer Analytics

Extracts data from PostgreSQL, transforms it into customer-level summaries,
validates data quality, and loads results to a database table and CSV file.
"""
# etl_pipeline.py

import pandas as pd
from sqlalchemy import create_engine
import os
from datetime import datetime

def extract(engine):
    """
    Extract all tables from PostgreSQL into Pandas DataFrames.
    Returns a dictionary: {"customers": df, "products": df, "orders": df, "order_items": df}
    """
    tables = ["customers", "products", "orders", "order_items"]
    data_dict = {}
    for table in tables:
        df = pd.read_sql_table(table, con=engine)
        data_dict[table] = df
        print(f"Extracted {len(df)} rows from {table}")
    return data_dict

def transform(data_dict):
    """
    Transform extracted data into customer-level summary.
    """
    customers = data_dict["customers"]
    products = data_dict["products"]
    orders = data_dict["orders"]
    order_items = data_dict["order_items"]

    # Join orders -> order_items -> products
    df = order_items.merge(orders, on="order_id", how="left") \
                    .merge(products, on="product_id", how="left") \
                    .merge(customers, on="customer_id", how="left")

    # Compute line_total
    df["line_total"] = df["quantity"] * df["unit_price"]

    # Filter out cancelled orders and suspicious quantities
    df = df[df["status"] != "cancelled"]
    df = df[df["quantity"] <= 100]

    # Aggregate to customer-level summary
    summary = df.groupby(["customer_id", "name"], as_index=False).agg(
        total_orders=pd.NamedAgg(column="order_id", aggfunc="nunique"),
        total_revenue=pd.NamedAgg(column="line_total", aggfunc="sum")
    )

    summary["avg_order_value"] = summary["total_revenue"] / summary["total_orders"]

    # Determine top category per customer
    category_revenue = df.groupby(["customer_id", "category"])["line_total"].sum().reset_index()
    top_category = category_revenue.sort_values(["customer_id", "line_total"], ascending=[True, False]) \
                                   .drop_duplicates("customer_id") \
                                   [["customer_id", "category"]]
    summary = summary.merge(top_category, on="customer_id")
    summary.rename(columns={"name": "customer_name", "category": "top_category"}, inplace=True)

    print(f"Transformed to {len(summary)} customer summary rows")
    return summary

def validate(df):
    """
    Run data quality checks. Returns a dict of check results. Raises ValueError if critical check fails.
    """
    checks = {}

    # No nulls in customer_id or customer_name
    checks["customer_id_not_null"] = df["customer_id"].notnull().all()
    checks["customer_name_not_null"] = df["customer_name"].notnull().all()

    # total_revenue > 0
    checks["total_revenue_positive"] = (df["total_revenue"] > 0).all()

    # No duplicate customer_id
    checks["unique_customer_id"] = df["customer_id"].is_unique

    # total_orders > 0
    checks["total_orders_positive"] = (df["total_orders"] > 0).all()

    # Print results
    for check, passed in checks.items():
        print(f"{check}: {'PASS' if passed else 'FAIL'}")

    # Raise if critical check fails
    if not all(checks.values()):
        raise ValueError("Validation failed. See output for details.")

    return checks

def load(df, engine, csv_path="output/customer_analytics.csv"):
    """
    Load the transformed DataFrame into PostgreSQL and CSV.
    """
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    df.to_sql("customer_analytics", con=engine, if_exists="replace", index=False)
    df.to_csv(csv_path, index=False)
    print(f"Loaded {len(df)} rows to customer_analytics table and CSV at {csv_path}")

def main():
    # Database connection
    engine = create_engine("postgresql+psycopg2://postgres:postgres@localhost:5432/amman_market")

    print("Starting ETL pipeline...")
    # Extract
    data_dict = extract(engine)
    # Transform
    customer_summary = transform(data_dict)
    # Validate
    validate(customer_summary)
    # Load
    load(customer_summary, engine)

    print("ETL pipeline completed successfully!")

if __name__ == "__main__":
    main()