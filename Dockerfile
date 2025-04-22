FROM apache/airflow:2.8.1

USER root

# Instala git con permisos de root
RUN apt-get update && apt-get install -y git

# Cambia al usuario airflow para instalar paquetes Python
USER airflow

# Instala dbt y dbt-snowflake con el usuario correcto
RUN pip install --no-cache-dir \
    dbt-core==1.8.7 \
    dbt-snowflake==1.8.4 \
    airflow-dbt-python==0.15.3
