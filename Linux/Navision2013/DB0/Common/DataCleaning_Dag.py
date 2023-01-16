from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.utils import timezone
from airflow.models import Variable
import pendulum
from pip._vendor.distlib.util import Configurator
import datetime as dt
import os, sys, subprocess
from os.path import dirname, join, abspath
import datetime as dt
import re

DBList = []
Script_Path = []

c = 'Common'
root_directory = abspath(join(join(dirname(__file__), '..', '..')))
kockpit_path = "#ConfiguratorInstallationDrive"

sys.path.insert(0, kockpit_path)
for folders in os.listdir(kockpit_path):
    if os.path.isdir(os.path.join(kockpit_path, folders)):
        if 'DB' in folders:
            DBList.append(folders)

DBList = sorted(DBList)
DBList.append(DBList.pop(DBList.index('DB0')))
for d in DBList:
    if 'DB0' in d:
        pass
    else:
        Connection = os.path.join(kockpit_path, d)
        sys.path.insert(0, Connection)
        for folders in os.listdir(Connection):
            if os.path.isdir(os.path.join(Connection, folders)):
                if 'E' in folders:
                    path = os.path.join(kockpit_path, d, folders, c)
                    Script_Path.append(path)
local_tz = pendulum.timezone("Asia/Calcutta")
dagargs = {
    'owner': 'kockpit',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 11, tzinfo=local_tz),
    'email': ['abhishek@kockpit.in'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'max_active_runs': 1,
    'end_date': datetime(2025, 2, 15, tzinfo=local_tz)
}

with DAG('DataCleaning',
         schedule_interval=None,
         default_args=dagargs,
         max_active_runs=1,
         is_paused_upon_creation=True,
         catchup=False) as dag:
    def Data_Cleaning():
        for i in range(len(Script_Path)):
            vacuum_ParquetData = BashOperator(
                task_id="vacuum_ParquetData" + str(i),
                wait_for_downstream=True,
                bash_command=" python3 " + Script_Path[i] + "/DataCleaning.py vacuum_ParquetData",
            )
            vacuum_Configurator = BashOperator(
                task_id="vacuum_Configurator" + str(i),
                wait_for_downstream=True,
                bash_command=" python3 " + Script_Path[i] + "/DataCleaning.py vacuum_Configurator",
            )
            vacuum_Masters = BashOperator(
                task_id="vacuum_Masters" + str(i),
                wait_for_downstream=True,
                bash_command=" python3 " + Script_Path[i] + "/DataCleaning.py Masters",
            )
            vacuum_Sales = BashOperator(
                task_id="vacuum_Sales" + str(i),
                wait_for_downstream=True,
                bash_command=" python3 " + Script_Path[i] + "/DataCleaning.py Sales",
            )
            vacuum_Purchase = BashOperator(
                task_id="vacuum_Purchase" + str(i),
                wait_for_downstream=True,
                bash_command=" python3 " + Script_Path[i] + "/DataCleaning.py Purchase",
            )
            vacuum_Finance = BashOperator(
                task_id="vacuum_Finance" + str(i),
                wait_for_downstream=True,
                bash_command=" python3 " + Script_Path[i] + "/DataCleaning.py Finance",
            )
            vacuum_Inventory = BashOperator(
                task_id="vacuum_Inventory" + str(i),
                wait_for_downstream=True,
                bash_command=" python3 " + Script_Path[i] + "/DataCleaning.py Inventory",
            )


    Data_Cleaning()

