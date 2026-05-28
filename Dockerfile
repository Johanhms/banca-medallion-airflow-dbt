FROM apache/airflow:2.8.1-python3.11

USER root
# Instalamos git por si dbt necesita descargar paquetes externos en el futuro
RUN apt-get update && apt-get install -y --no-install-recommends git && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
# Instalamos dbt core y el adaptador específico para Postgres
RUN pip install --no-cache-dir dbt-core dbt-postgres