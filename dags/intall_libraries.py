from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta


# Definir el DAG
with DAG(
    dag_id="install_libraries_airflow",
    start_date=days_ago(1),
    schedule=None,  # Se ejecuta manualmente
    catchup=False,
) as dag:

    install_postgres_provider = BashOperator(
        task_id="install_sap_lib",
        bash_command="pip install dbt-core dbt-snowflake airflow-dbt-python",
    )

    install_postgres_provider