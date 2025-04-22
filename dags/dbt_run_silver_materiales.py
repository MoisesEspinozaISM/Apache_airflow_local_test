from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_PROJECT_DIR = "/opt/airflow/dbt/test_dbt_snowflake"
DBT_PROFILE_DIR = "/home/airflow/.dbt"

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='dbt_run_silver_materiales',
    default_args=default_args,
    description='DAG para ejecutar modelo materiales_enriquecidos en silver',
    schedule_interval=None,
    start_date=datetime(2025, 4, 22),
    catchup=False,
    tags=['dbt', 'silver', 'materiales']
) as dag:

    install_dbt_deps = BashOperator(
        task_id='install_dbt_deps',
        bash_command=f"""
        cd {DBT_PROJECT_DIR} && \
        dbt deps --profiles-dir {DBT_PROFILE_DIR}
        """
    )

    run_silver_materiales = BashOperator(
        task_id='run_materiales_enriquecidos',
        bash_command=f"""
        cd {DBT_PROJECT_DIR} && \
        dbt run --select materiales.silver.materiales_enriquecidos --profiles-dir {DBT_PROFILE_DIR}
        """
    )

    test_silver_materiales = BashOperator(
        task_id='test_materiales_enriquecidos',
        bash_command=f"""
        cd {DBT_PROJECT_DIR} && \
        dbt test --select materiales.silver.materiales_enriquecidos --profiles-dir {DBT_PROFILE_DIR}
        """
    )

    install_dbt_deps >> run_silver_materiales >> test_silver_materiales
