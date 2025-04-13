from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='dbt_stg_clientes_pipeline',
    default_args=default_args,
    schedule_interval=None,  # Ejecutar manualmente
    catchup=False,
    tags=["dbt", "clientes", "staging"]
) as dag:

    dbt_run_stg_clientes = BashOperator(
        task_id='dbt_run_stg_clientes',
        bash_command='cd /opt/airflow/dbt && dbt run --select clientes.staging.stg_clientes_demo'
    )

    dbt_test_stg_clientes = BashOperator(
        task_id='dbt_test_stg_clientes',
        bash_command='cd /opt/airflow/dbt && dbt test --select clientes.staging.stg_clientes_demo'
    )

    dbt_run_stg_clientes >> dbt_test_stg_clientes
