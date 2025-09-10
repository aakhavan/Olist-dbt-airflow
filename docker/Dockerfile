# Use a specific version of Airflow for reproducibility
FROM apache/airflow:2.8.1-python3.9

USER root
RUN apt-get update && apt-get install -y --no-install-recommends git && apt-get clean
USER airflow

# Install dbt-postgres and the Postgres provider for Airflow
# Pinning versions is a best practice for production environments
RUN pip install --no-cache-dir apache-airflow-providers-postgres==5.10.0 dbt-postgres==1.7.9 pandas==2.1.4 sqlalchemy==1.4.50