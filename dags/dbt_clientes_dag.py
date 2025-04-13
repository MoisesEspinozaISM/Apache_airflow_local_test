from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='dbt_clientes_pipeline',
    default_args=default_args,
    schedule_interval=None,  # Puedes cambiarlo a '0 6 * * *' para ejecución diaria a las 6 AM
    catchup=False,
    tags=["dbt", "clientes"],
    description="Pipeline para ejecutar y testear modelos DBT del dominio 'clientes'"
) as dag:

    # Ejecuta modelos del dominio clientes
    dbt_run_clientes = BashOperator(
        task_id='dbt_run_clientes',
        bash_command='cd /opt/airflow/dbt && dbt run --select clientes',
        dag=dag
    )

    # Testea los modelos luego de la ejecución
    dbt_test_clientes = BashOperator(
        task_id='dbt_test_clientes',
        bash_command='cd /opt/airflow/dbt && dbt test --select clientes',
        dag=dag
    )

    # Define el flujo de tareas
    dbt_run_clientes >> dbt_test_clientes
