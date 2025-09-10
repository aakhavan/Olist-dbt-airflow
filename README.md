# Local ELT Project with Airflow, dbt, and PostgreSQL

This project is a self-contained, local ELT (Extract, Load, Transform) pipeline designed to showcase senior-level data engineering skills using modern tools. It uses the Olist Brazilian E-commerce dataset from Kaggle.

## Architecture

The project uses Docker Compose to orchestrate three main components:

1.  **PostgreSQL**: Acts as the data warehouse. It stores the raw data, and dbt creates transformed models within it.
2.  **Apache Airflow**: The orchestrator that schedules and runs the ELT pipeline. It runs a Python script to load data and then triggers dbt commands.
3.  **dbt (Data Build Tool)**: Used for the "T" (Transform) in ELT. It transforms the raw data loaded by Airflow into clean, analytics-ready tables.

The pipeline flow is as follows:
1.  An Airflow DAG is triggered (manually in this case).
2.  A Python task in Airflow reads CSV files from the local `data/` directory and loads them into a `raw` schema in PostgreSQL.
3.  A `dbt run` task is triggered, transforming the raw data into staging and mart models. This includes an **incremental model** for `fct_orders` that only processes new data on subsequent runs.
4.  A `dbt test` task runs to ensure the integrity and quality of the transformed data, including a **custom generic test**.

## Key Features & Skills Demonstrated

*   **Containerization**: Entire environment is defined in `docker-compose.yml` for easy setup and reproducibility.
*   **Orchestration**: Airflow DAG orchestrates the entire pipeline with clear dependencies.
*   **Idempotent Data Loading**: The loading script is designed to be re-runnable without side effects.
*   **Advanced dbt**:
    *   **Incremental Models**: `fct_orders` is built incrementally to efficiently process new data, a crucial skill for large datasets.
    *   **Custom Generic Tests**: `assert_is_positive` shows how to extend dbt's testing framework for custom data quality checks.
    *   **Macros**: A `cents_to_dollars` macro demonstrates how to create reusable SQL logic.
    *   **Explicit Profiles**: A `profiles.yml` is included to manage database connections robustly.

## Prerequisites

*   Docker and Docker Compose installed.
*   The Olist dataset CSV files from Kaggle.

## Setup and Execution

**Step 1: Prepare the Data**

1.  Create a `data/` directory in the root of this project.
2.  Download the Olist dataset from Kaggle.
3.  Place the following CSV files into the `data/` directory:
    *   `olist_customers_dataset.csv`
    *   `olist_orders_dataset.csv`
    *   `olist_order_items_dataset.csv`
    *   `olist_order_payments_dataset.csv`

**Step 2: A Note for Windows Users (Line Endings)**

When you create `docker/postgres-init.sh` on Windows, your editor might use Windows-style line endings (CRLF). The Linux container requires Unix-style endings (LF).

**To fix this**, open `docker/postgres-init.sh` in your code editor (like VS Code or PyCharm), and look in the bottom-right status bar. If it says `CRLF`, click on it and change it to `LF`. Save the file.

**Step 3: Build and Run the Docker Containers**

Open your terminal in the project's root directory (`C:\Users\amir0\PycharmProjects\OList_dbt_airflow`) and run:

```bash
# Build the custom Airflow image and start all services in detached mode
docker-compose up --build -d
```

This command will:
1.  Build the custom Airflow image with dbt installed.
2.  Start the PostgreSQL and Airflow services.
3.  Initialize the Airflow metadata database and create a default `admin` user.

It may take a few minutes for all services to become healthy.

**Step 4: Trigger the Airflow DAG**

1.  Open your web browser and navigate to the Airflow UI: `http://localhost:8080`.
2.  Log in with the username `admin` and password `admin`.
3.  On the main DAGs page, you will see `olist_elt_pipeline`. Un-pause it by clicking the toggle on the left.
4.  Click on the DAG name and then click the "Play" button (▶️) on the top right to trigger a new DAG run.

You can monitor the progress of the run in the "Grid" view.

**Step 5: Verify the Results in PostgreSQL**

Once the DAG run completes successfully, the transformed tables will be available in the `olist` database. You can connect to Postgres using any SQL client (like DBeaver, DataGrip, or `psql`) with these credentials:

*   **Host**: `localhost`
*   **Port**: `5432`
*   **Database**: `olist`
*   **User**: `postgres`
*   **Password**: `postgres`

Query the final fact table to see the result:

```sql
SELECT * FROM marts.fct_orders LIMIT 10;
```

You can also check the raw tables:

```sql
SELECT * FROM raw.olist_orders_dataset LIMIT 10;
```

## Cleaning Up

To stop and remove all containers, networks, and volumes, run:

```bash
docker-compose down --volumes
```