from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='dbt_debug_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["dbt", "debug"]
) as dag:

    dbt_debug = BashOperator(
        task_id='dbt_debug',
        bash_command='cd /opt/airflow/dbt && dbt debug'
    )
