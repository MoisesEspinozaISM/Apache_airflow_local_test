from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='dbt_clientes_pipeline',
    default_args=default_args,
    description='Pipeline de clientes con dbt',
    schedule_interval='@daily',
    catchup=False,
    tags=['dbt', 'clientes'],
) as dag:

    # Ejecuta solo los modelos dentro de models/clientes/
    dbt_run_clientes = BashOperator(
        task_id='dbt_run_clientes',
        bash_command='cd /opt/airflow/dbt && dbt run --select clientes'
    )

    dbt_test_clientes = BashOperator(
        task_id='dbt_test_clientes',
        bash_command='cd /opt/airflow/dbt && dbt test --select clientes'
    )

    dbt_run_clientes >> dbt_test_clientes
