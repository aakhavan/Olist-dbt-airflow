# Local ELT Project with Airflow, dbt, ClickHouse, and Superset

This project is a self-contained, local ELT (Extract, Load, Transform) pipeline designed to showcase a modern, professional data engineering architecture for a job interview. It uses the Olist Brazilian E-commerce dataset from Kaggle.

## Architecture

The project uses Docker Compose to orchestrate a decoupled, containerized environment:

1.  **PostgreSQL (`postgres_airflow`)**: Serves exclusively as the metadata backend for Airflow. It is completely separate from the data pipeline.
2.  **ClickHouse (`clickhouse-server`)**: The primary data warehouse (DWH). It's a high-performance, column-oriented database ideal for analytics.
3.  **Apache Airflow (`airflow`)**: The orchestrator. It runs a simple `standalone` instance and uses the `DockerOperator` to delegate tasks to other containers, keeping the Airflow environment clean and focused.
4.  **dbt (`dbt-clickhouse-olist` image)**: The transformation tool. dbt runs inside its own ephemeral Docker container, triggered by Airflow. This decouples the transformation logic from the orchestrator.
5.  **Apache Superset (`superset`)**: The business intelligence tool used to visualize the final, transformed data from ClickHouse.

### Pipeline Flow

1.  An Airflow DAG is triggered (manually in this case).
2.  A Python task loads the raw Olist CSV files into a `raw` schema in ClickHouse.
3.  A `DockerOperator` task builds the dbt image.
4.  A `DockerOperator` task starts a new container from the dbt image and executes `dbt run` to transform the raw data into analytics-ready tables (`staging` and `marts` layers).
5.  A final `DockerOperator` task runs `dbt test` to ensure data quality.

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

**Step 2: Build and Run the Docker Containers**

Open your terminal in the project's root directory and run:

```bash
# Build and start all services in detached mode
docker-compose up --build -d
```

This command will:
1.  Start PostgreSQL, ClickHouse, Airflow, and Superset services.
2.  Initialize the Airflow and Superset metadata databases.
3.  Automatically configure Superset to connect to the ClickHouse DWH.

It may take a few minutes for all services to become healthy.

**Step 3: Trigger the Airflow DAG**

1.  Open your web browser and navigate to the Airflow UI: `http://localhost:8080`.
2.  Log in with the username `airflow_admin` and password `airflow_admin`.
3.  On the main DAGs page, find `olist_elt_pipeline`. Un-pause it by clicking the toggle on the left.
4.  Click the "Play" button (▶️) on the right to trigger a new DAG run.

You can monitor the progress of the run in the "Grid" view.

**Step 4: Visualize in Superset**

Once the DAG run completes successfully, the transformed tables will be available in ClickHouse and ready to be visualized in Superset.

1.  Navigate to the Superset UI: `http://localhost:8888`.
2.  Log in with the username `admin` and password `admin`.
3.  The `Olist DWH` data source should already be connected.
4.  Click the **+** button in the top right and select **Chart**.
5.  Choose the `mart_customer_revenue` table from the `Olist DWH` datasource.
6.  Create a simple chart:
    *   **Chart Type**: Table
    *   **Columns**: `customer_unique_id`, `number_of_orders`, `total_revenue`
    *   Click **Create Chart**.

You can now explore the transformed data and build dashboards.

## Cleaning Up

To stop and remove all containers, networks, and volumes (including all database data), run:

```bash
docker-compose down --volumes
```