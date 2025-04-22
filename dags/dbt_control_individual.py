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
    dag_id='dbt_control_individual_models',
    default_args=default_args,
    description='DAG para ejecutar modelos dbt individuales con BashOperator',
    schedule_interval=None,
    start_date=datetime(2025, 4, 22),
    catchup=False,
    tags=['dbt', 'selectivos', 'materiales']
) as dag:

    install_deps = BashOperator(
        task_id='install_dbt_dependencies',
        bash_command=f"""
        cd {DBT_PROJECT_DIR} && \
        dbt deps --profiles-dir {DBT_PROFILE_DIR}
        """
    )

    run_makt = BashOperator(
        task_id='run_sapprd_makt',
        bash_command=f"""
        cd {DBT_PROJECT_DIR} && \
        dbt run --select test_dbt_snowflake.materiales.staging.sapprd_makt --profiles-dir {DBT_PROFILE_DIR}
        """
    )

    run_mara = BashOperator(
        task_id='run_sapprd_mara',
        bash_command=f"""
        cd {DBT_PROJECT_DIR} && \
        dbt run --select test_dbt_snowflake.materiales.staging.sapprd_mara --profiles-dir {DBT_PROFILE_DIR}
        """
    )

    run_marc = BashOperator(
        task_id='run_sapprd_marc',
        bash_command=f"""
        cd {DBT_PROJECT_DIR} && \
        dbt run --select test_dbt_snowflake.materiales.staging.sapprd_marc --profiles-dir {DBT_PROFILE_DIR}
        """
    )

    test_all = BashOperator(
        task_id='test_materiales_staging',
        bash_command=f"""
        cd {DBT_PROJECT_DIR} && \
        dbt test --select test_dbt_snowflake.materiales.staging.sapprd_makt \
                          test_dbt_snowflake.materiales.staging.sapprd_mara \
                          test_dbt_snowflake.materiales.staging.sapprd_marc \
                          --profiles-dir {DBT_PROFILE_DIR}
        """
    )

    install_deps >> run_makt >> [run_mara, run_marc] >> test_all
