FROM python:3.9-slim

WORKDIR /app

RUN pip install --no-cache-dir pandas==2.2.1 "SQLAlchemy<2.0" psycopg2-binary==2.9.9

# Copy the script that will be run
COPY scripts/load_raw_data.py /app/load_raw_data.py

# The script will be the entrypoint
CMD ["python", "load_raw_data.py"]