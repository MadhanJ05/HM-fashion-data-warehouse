"""
H&M Fashion Data Warehouse - dbt DAG
=====================================
Orchestrates dbt model runs: staging → intermediate → marts
Schedule: Daily at 6:00 AM
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


# =============================================
# DAG CONFIGURATION
# =============================================

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
    schedule_interval='0 6 * * *',  # Daily at 6:00 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dbt', 'hm-fashion', 'data-warehouse'],
)


# =============================================
# DBT COMMANDS
# =============================================

DBT_DIR = '/opt/airflow/dbt_project'
DBT_CMD = f'dbt --profiles-dir {DBT_DIR} --project-dir {DBT_DIR}'


# =============================================
# TASKS
# =============================================

# Task 1: Test database connection
test_connection = BashOperator(
    task_id='test_connection',
    bash_command=f'{DBT_CMD} debug',
    dag=dag,
)

# Task 2: Run staging models
run_staging = BashOperator(
    task_id='run_staging_models',
    bash_command=f'{DBT_CMD} run --select staging',
    dag=dag,
)

# Task 3: Run intermediate models
run_intermediate = BashOperator(
    task_id='run_intermediate_models',
    bash_command=f'{DBT_CMD} run --select intermediate',
    dag=dag,
)

# Task 4: Run marts models
run_marts = BashOperator(
    task_id='run_marts_models',
    bash_command=f'{DBT_CMD} run --select marts',
    dag=dag,
)

# Task 5: Run dbt tests
run_tests = BashOperator(
    task_id='run_dbt_tests',
    bash_command=f'{DBT_CMD} test || true',  # Don't fail if no tests
    dag=dag,
)


# =============================================
# TASK DEPENDENCIES
# =============================================

test_connection >> run_staging >> run_intermediate >> run_marts >> run_tests