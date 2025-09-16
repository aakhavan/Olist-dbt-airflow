# Olist E-commerce ELT Pipeline

This project demonstrates a complete, containerized ELT pipeline for the Olist e-commerce dataset. The primary goal is to showcase a modern data stack using **dbt** for transformation, **Airflow** for orchestration, and **ClickHouse** as an analytical data warehouse.

## Architecture

The pipeline follows a simple, robust architecture, with each component running in its own Docker container.

```
┌──────────┐     ┌──────────────────┐     ┌─────────────┐     ┌────────────────┐
│          │     │                  │     │             │     │                │
│   CSVs   ├────►│  Airflow (dbt)   ├────►│  ClickHouse │◄───┤ BI (Python)  │
│          │     │ (Orchestration)  │     │  (Warehouse)│     │ (Visualization)│
└──────────┘     └──────────────────┘     └─────────────┘     └────────────────┘
```

* **Data Source**: Raw CSV files from the Olist E-commerce dataset.
* **Orchestration**: Apache Airflow triggers and monitors the dbt transformation jobs.
* **Transformation**: dbt models clean, transform, and structure the raw data into analytical tables.
* **Data Warehouse**: ClickHouse stores the transformed data, ready for querying.
* **BI & Visualization**: A Python script (`dashboard.py`) connects to ClickHouse to run analytical queries and generate insights.

***

## Getting Started

### Prerequisites

* Docker and Docker Compose installed on your local machine.

### 1. Download the Dataset

This project uses the **Brazilian E-Commerce Public Dataset by Olist**.

* **Download Link**: [https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
* **Instructions**: Download the `.zip` file, extract it, and place all the `.csv` files inside the `data/` directory at the root of this project.

### 2. Run the Pipeline

Once the data is in place, you can start the entire pipeline with a single command:

```bash
docker-compose up --build
```

This command will:
1.  Build the custom Docker images for Airflow, dbt, and our BI script.
2.  Start all services (Airflow, dbt, ClickHouse, Superset).
3.  Automatically trigger the Airflow DAG to run the dbt models and populate ClickHouse.

You can access the Airflow UI at `http://localhost:8080` (username/password: `airflow`).

***

## Project Structure

```
.
├── airflow/                # Airflow configurations, DAGs, and Dockerfile
├── dashboard/              # Python script for BI dashboard
├── data/                   # (Git-ignored) Raw Olist CSV files go here
├── dbt/                    # dbt project, models, and configurations
├── docker-compose.yml      # Defines all services and their interactions
└── README.md
```