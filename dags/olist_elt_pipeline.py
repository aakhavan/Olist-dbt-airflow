import os
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

# Define constants for file paths and database connection
# These paths are inside the Airflow container
DBT_PROJECT_DIR = "/usr/local/airflow/dbt_project"
DATA_DIR = "/usr/local/airflow/data"
DB_CONN_STR = "postgresql+psycopg2://postgres:postgres@postgres:5432/olist"

# Define the list of CSV files to be loaded
CSV_FILES = [
    "olist_customers_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
]


@dag(
    dag_id="olist_elt_pipeline",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["olist", "dbt"],
    doc_md="""
    ### Olist ELT Pipeline
    This pipeline performs the following steps:
    1. **Load Raw Data**: Loads Olist CSV files from the `data/` directory into the `raw` schema in PostgreSQL.
    2. **dbt Run**: Executes `dbt run` to transform the raw data into staging and mart models.
    3. **dbt Test**: Executes `dbt test` to validate the transformed data.
    """,
)
def olist_elt_pipeline():
    """
    Orchestrates the ELT process for the Olist dataset.
    """

    @task
    def load_raw_data():
        """
        Loads Olist CSV data into the 'raw' schema of the PostgreSQL database.
        This task is idempotent: it truncates and replaces the data on each run.
        """
        engine = create_engine(DB_CONN_STR)
        
        # Create the 'raw' schema if it doesn't exist
        with engine.connect() as conn:
            conn.execute("CREATE SCHEMA IF NOT EXISTS raw;")

        for file_name in CSV_FILES:
            file_path = os.path.join(DATA_DIR, file_name)
            table_name = file_name.replace(".csv", "")

            if not os.path.exists(file_path):
                raise FileNotFoundError(f"CSV file not found at {file_path}")

            df = pd.read_csv(file_path)
            
            print(f"Loading {len(df)} rows into raw.{table_name}...")
            
            # Load data into a table in the 'raw' schema
            df.to_sql(
                table_name,
                engine,
                schema="raw",
                if_exists="replace",
                index=False,
                chunksize=10000,
            )
            print(f"Successfully loaded data into raw.{table_name}.")


    # dbt tasks using BashOperator
    # We specify the project and profiles directory for dbt
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}",
    )

    # Define task dependencies
    load_raw_data() >> dbt_run >> dbt_test


# Instantiate the DAG
olist_elt_pipeline()