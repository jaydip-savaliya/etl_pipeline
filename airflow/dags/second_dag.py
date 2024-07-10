from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow import DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'practice_dag_v1',
    default_args=default_args,
    description='An ELT workflow with dbt',
    start_date=datetime(2024, 6, 26),
    catchup=False,
)

t1 = BashOperator(
    task_id="first",
    bash_command="echo first",
    dag=dag
)
t2 = BashOperator(
    task_id="second",
    bash_command="echo second",
    dag=dag
)
t3 = BashOperator(
    task_id="third",
    bash_command="echo third",
    dag=dag
)
t4 = BashOperator(
    task_id="forth",
    bash_command="echo forth",
    dag=dag
)
t5 = BashOperator(
    task_id="fifth",
    bash_command="echo forth",
    dag=dag
)
t6 = BashOperator(
    task_id="sixth",
    bash_command="echo forth",
    dag=dag
)
t7 = BashOperator(
    task_id="seventh",
    bash_command="echo forth",
    dag=dag
)


t1 >> t2
t2 >> [t3,t4,t5]
t6 >> t4 >> t5
t5 >> t7
