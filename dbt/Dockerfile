FROM python:3.12-slim

WORKDIR /dbt_project

RUN pip install dbt-clickhouse==1.8.0

COPY ./dbt_project/ .

ENTRYPOINT [ "dbt" ]