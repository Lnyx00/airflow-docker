import json

import pendulum
from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import requests
import pandas as pd

args = {
    'start_date': datetime(2022, 1, 23,
                           tzinfo=pendulum.timezone("Asia/Bangkok")),
    'email': ['team@example.com'],
    'email_on_failure': False
}

dag = DAG(
    dag_id='hello-world',
    default_args=args,
    schedule_interval=None,
    max_active_runs=1,
    catchup=False
)

task_hello_word = BashOperator(
    task_id='task_hello_world',
    bash_command='echo "hello world"',
    dag=dag,
)

def hitAPI():
    hit = requests.get('https://covid19-public.digitalservice.id/api/v1/rekapitulasi_v2/jabar/harian')
    return hit.json()

def hitAPI2(**context):
    hit = requests.get('https://covid19-public.digitalservice.id/api/v1/rekapitulasi_v2/jabar/harian').json()
    getContent = hit['data']['content']
    # jsonText = json.dumps(hit,indent=4)
    context['ti'].xcom_push(key='hasilAPI',value=getContent)
    return getContent

def pandasView(**kwargs):
    ti = kwargs['ti']
    ls = ti.xcom_pull(task_ids='runAPI2')
    df = pd.json_normalize(ls)
    return df


def createCSV(hasilJson):
    f = open("demofile2.txt", "w")
    f.write(hasilJson)
    f.close()

def hitung ():
    return 1 + 1

with dag:
    hitAPI = PythonOperator(
        task_id = 'runAPI',
        python_callable= hitAPI,
        provide_context = True
    )

    hitAPI2 = PythonOperator(
        task_id = 'runAPI2',
        python_callable= hitAPI2,
        provide_context = True
    )

    createDF = PythonOperator(
        task_id= 'Get_JSON_and_Convert_to_DF',
        python_callable=pandasView,
        provide_context= True
    )

    creteCSV = PythonOperator(
        task_id = 'CreateCSV',
        python_callable=hitung,
        provide_context= True
    )

    hitAPI2 >> createDF



