"""
H&M Fashion Data Warehouse - dbt DAG
Orchestrates dbt model runs: staging -> intermediate -> marts
Schedule: Daily at 6:00 AM
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    dag_id='hm_dbt_pipeline',
    default_args=default_args,
    description='Run dbt models for H&M fashion data warehouse',
    schedule_interval='0 6 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dbt', 'hm-fashion', 'data-warehouse'],
)

DBT_DIR = '/opt/airflow/dbt_project'

run_staging = BashOperator(
    task_id='run_staging_models',
    bash_command=f'dbt run --profiles-dir {DBT_DIR} --project-dir {DBT_DIR} --select staging',
    dag=dag,
)

run_intermediate = BashOperator(
    task_id='run_intermediate_models',
    bash_command=f'dbt run --profiles-dir {DBT_DIR} --project-dir {DBT_DIR} --select intermediate',
    dag=dag,
)

run_marts = BashOperator(
    task_id='run_marts_models',
    bash_command=f'dbt run --profiles-dir {DBT_DIR} --project-dir {DBT_DIR} --select marts',
    dag=dag,
)

run_tests = BashOperator(
    task_id='run_dbt_tests',
    bash_command=f'dbt test --profiles-dir {DBT_DIR} --project-dir {DBT_DIR} || true',
    dag=dag,
)

run_staging >> run_intermediate >> run_marts >> run_tests
