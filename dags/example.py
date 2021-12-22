from datetime import datetime, timedelta
import pandas as pd
import os

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def read_csv():
    AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
    file = pd.read_csv(AIRFLOW_HOME+ '/resources/test.csv')
    print("CSV File contents " + file)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
with DAG(
    'Example',
    default_args=default_args,
    description='example DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag: 

    t2 = PythonOperator(
        task_id="read_csv",
        python_callable=read_csv
    )

    t3 = BashOperator(
        task_id='show_working_directory',
        bash_command = 'pwd'
    )

    t4 = BashOperator(
        task_id='show_files',
        bash_command = 'ls -als /opt/airflow/resources'
    )

    t3 >> t4 >> t2

