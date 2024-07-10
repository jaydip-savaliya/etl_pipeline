from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from docker.types import Mount
import subprocess


import subprocess
 
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'etl_pipeline_v5.02',
    default_args=default_args,
    description='An ELT workflow with dbt',
    start_date=datetime(2024, 6, 26),
    catchup=False,
)

def run_etl_script():
    script_path = "/opt/airflow/etl_script/etl_script.py"
    result = subprocess.run(["python", script_path],
                            capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Script failed with error: {result.stderr}")
    else:
        print(result.stdout)

t1 = DockerOperator(
    task_id="dbt_run_with_snowflake",
    image='ghcr.io/dbt-labs/dbt-snowflake:latest',
    command=[
        "run",
        "--profiles-dir",
        "/root",
        "--project-dir",
        "/opt/dbt",
        "--full-refresh"
    ],
    auto_remove=True,
    docker_url="tcp://docker-proxy:2375",
    mounts=[
        Mount(source='/home/mihir/Downloads/ETL Pipeline/etl_pipeline',
              target='/opt/dbt', type='bind'),
        Mount(source='/home/mihir/.dbt', target='/root', type='bind'),
    ],
    dag=dag
)

t2 = PythonOperator(
    task_id="transfer_task",
    python_callable=run_etl_script,
    dag=dag
)

t1 >> t2