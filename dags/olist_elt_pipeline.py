import os
from datetime import datetime
import time
import requests
import pandas as pd
import clickhouse_connect

from airflow.decorators import dag, task
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from schemas import schemas as RAW_TABLE_SCHEMAS

# Securely load credentials from environment variables
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse-server")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", 8123))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "admin")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "admin")
DOCKER_NETWORK = "etl_network"
DBT_IMAGE_NAME = "dbt-clickhouse-olist:latest"


def _ensure_table(client, db: str, table: str, schema: dict) -> None:
    cols = ",\n  ".join(f"`{col_name}` {col_type}" for col_name, col_type in schema.items())
    client.command(f"CREATE DATABASE IF NOT EXISTS {db}")
    client.command(
        f"""
        CREATE OR REPLACE TABLE {db}.{table} (
          {cols}
        )
        ENGINE = MergeTree
        ORDER BY tuple()
        """
    )

@dag(
    dag_id="olist_elt_pipeline",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["olist", "dbt", "clickhouse"],
)
def olist_elt_pipeline():
    @task
    def wait_for_clickhouse():
        url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/ping"
        for _ in range(60):
            try:
                r = requests.get(url, timeout=2)
                if r.status_code == 200 and r.text.strip() == "Ok.":
                    return
            except Exception:
                pass
            time.sleep(2)
        raise RuntimeError("ClickHouse not reachable on network 'elt_network'")

    @task
    def load_raw_data():
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
        )

        data_dir = "/opt/airflow/data"
        csv_files = [f for f in os.listdir(data_dir) if f.endswith(".csv")]

        for file_name in csv_files:
            file_path = os.path.join(data_dir, file_name)
            table_name = file_name.replace(".csv", "")

            if table_name not in RAW_TABLE_SCHEMAS:
                print(f"Skipping file with no defined schema: {file_name}")
                continue

            schema = RAW_TABLE_SCHEMAS[table_name]

            try:
                df = pd.read_csv(file_path, encoding="utf-8", dtype=str, keep_default_na=False, na_values=[''])
                if df.empty:
                    print(f"Skipping empty file: {file_name}")
                    continue
                df.columns = [c.replace("-", "_") for c in df.columns]
                df = df[schema.keys()]

                _ensure_table(client, "raw", table_name, schema)
                client.insert_df(table=table_name, df=df, database="raw")
                print(f"Loaded {len(df)} rows into raw.{table_name}")
            except Exception as e:
                print(f"Skipping file due to error: {file_name} - {e}")

    dbt_run = DockerOperator(
        task_id="dbt_run",
        image=DBT_IMAGE_NAME,
        command="run --profiles-dir .",
        network_mode=DOCKER_NETWORK,  # now guaranteed to exist as 'elt_network'
        auto_remove=True,
        mount_tmp_dir=False,
    )

    dbt_test = DockerOperator(
        task_id="dbt_test",
        image=DBT_IMAGE_NAME,
        command="test --profiles-dir .",
        network_mode=DOCKER_NETWORK,
        auto_remove=True,
        mount_tmp_dir=False,
    )

    wait_for_clickhouse() >> load_raw_data() >> dbt_run >> dbt_test

olist_elt_pipeline()