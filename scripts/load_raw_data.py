import os
import pandas as pd
from sqlalchemy import create_engine, text

# Get the database connection string from an environment variable
DB_CONN_STR = os.environ.get("DB_CONN_STR")
if not DB_CONN_STR:
    raise ValueError("DB_CONN_STR environment variable not set.")

# These paths are relative to the container's file system
DATA_DIR = "/data"
CSV_FILES = [
    "olist_customers_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
]

def load_data():
    """
    Loads Olist CSV data into the 'raw' schema of the PostgreSQL database.
    This function is idempotent: it replaces the data on each run.
    """
    engine = create_engine(DB_CONN_STR)

    # Create the 'raw' schema if it doesn't exist
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))

    for file_name in CSV_FILES:
        file_path = os.path.join(DATA_DIR, file_name)
        table_name = file_name.replace(".csv", "")

        df = pd.read_csv(file_path)
        print(f"Loading {len(df)} rows into raw.{table_name}...")

        df.to_sql(table_name, engine, schema="raw", if_exists="replace", index=False, chunksize=10000)
        print(f"Successfully loaded data into raw.{table_name}.")

if __name__ == "__main__":
    load_data()