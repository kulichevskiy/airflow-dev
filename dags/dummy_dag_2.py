from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

# Define default arguments for the DAG
default_args = {
    'owner': 'Alex',
    'depends_on_past': False,
    'email': ['alex@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG, its scheduling, and set it to start in the past
dag = DAG(
    'dummy_dag_2',
    default_args=default_args,
    description='Another simple dummy DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 3, 14),
    catchup=False,
)

# Define the tasks
start_task = DummyOperator(
    task_id='start_task',
    dag=dag,
)

end_task = DummyOperator(
    task_id='end_task',
    dag=dag,
)

# Set task dependencies
start_task >> end_task
