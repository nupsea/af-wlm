FROM quay.io/astronomer/astro-runtime:9.1.0

ENV AIRFLOW_VAR_ENV='dev'
ENV AIRFLOW_VAR_NOVELS_DATA_SOURCE='{"id":"source","file_path":"/opt/data/novels"}'
