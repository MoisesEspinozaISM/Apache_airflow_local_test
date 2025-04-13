from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='dbt_clientes_pipeline',
    default_args=default_args,
    schedule_interval=None,  # o ponlo como '0 6 * * *' para diario a las 6am
    catchup=False,
    tags=["dbt", "clientes"]
) as dag:

    dbt_run_clientes = BashOperator(
        task_id='dbt_run_clientes',
        bash_command='cd /opt/airflow/dbt && dbt run --select clientes'
    )

    dbt_test_clientes = BashOperator(
        task_id='dbt_test_clientes',
        bash_command='cd /opt/airflow/dbt && dbt test --select clientes'
    )

    dbt_run_clientes >> dbt_test_clientes
