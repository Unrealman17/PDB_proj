import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from dag_helper import Chdir, init_project

path = init_project('pdb_proj')

dag = DAG(dag_id="install_run",
          start_date=datetime(2021, 1, 1),
          schedule_interval=None,
          catchup=False,
          tags=['pdb_proj'])

install = BashOperator(
    dag=dag,
    task_id="install",
    bash_command=f'cd {path} && python install.py')

fill_config_table = BashOperator(
    dag=dag,
    task_id="fill_config_table",
    bash_command=f'cd {path} && python fill_config_table.py')

configure = BashOperator(
    dag=dag,
    task_id="configure",
    bash_command=f'cd {path} && python configure.py')


main = BashOperator(
    dag=dag,
    task_id="main",
    bash_command=f'cd {path} && python main.py')

with Chdir(path):
    with open('config.json') as f:
        j = f.read()
    download_thread = json.loads(j)["download_thread"]
    download = [
        BashOperator(
            dag=dag,
            task_id=f"download_{i}",
            bash_command=f'cd {path} && python download_file.py {i}'
        )
        for i in range(download_thread)
    ]

install >> fill_config_table 
fill_config_table >> configure
configure >> download
download >> main


print('updated: ',datetime.now().time())
