from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
}

with DAG(
    dag_id='dbt_connection_check',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['dbt', 'check']
) as dag:

    check_dbt_connection = BashOperator(
        task_id='run_dbt_debug',
        bash_command='cd /opt/airflow/dbt/test_dbt_snowflake && dbt debug',
    )
