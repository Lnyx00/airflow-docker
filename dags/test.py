import pendulum
from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import task
import requests
import pandas as pd

args = {
    'start_date': datetime(2022, 1, 23,
                           tzinfo=pendulum.timezone("Asia/Bangkok")),
    'email': ['team@example.com'],
    'email_on_failure': False
}
dag = DAG(
    dag_id='Tugas_FINAL',
    default_args=args,
    schedule_interval=None,
    max_active_runs=1,
    catchup=False

)
@task(task_id="print_the_context")
def print_context(ds=None, **kwargs):
    """Print the Airflow context and ds variable from the context."""
    print(kwargs)
    print(ds)
    return 'Whatever you return gets printed in the logs'

run_this = print_context()