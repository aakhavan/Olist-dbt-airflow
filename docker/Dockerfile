# Use a specific version of Airflow for reproducibility
FROM apache/airflow:2.8.1-python3.9

USER root
RUN apt-get update && apt-get install -y --no-install-recommends git postgresql-client libpq-dev && apt-get clean
USER airflow

# Copy and install dependencies from requirements.txt, which is the single source of truth
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt