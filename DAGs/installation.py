from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from dag_helper import Chdir, init_project
from airflow.operators.bash import BashOperator

'''

def helloWorld():
    print('Hello World')

dag = DAG(dag_id="hello",
         start_date=datetime(2021,1,1),
         schedule_interval="@hourly",
         catchup=False)


task1 = PythonOperator(
    dag = dag,
    task_id="hello_world",
    python_callable=helloWorld)


task1

'''

path = init_project('pdb_proj')


with DAG(dag_id="installation",
         start_date=datetime(2021,1,1),
         schedule_interval=None,
         catchup=False,
         tags=['pdb_proj'],):

    install = BashOperator(
        task_id="install",
        bash_command=f'cd {path} && python install.py')

    install
# '''