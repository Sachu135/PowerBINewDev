from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.utils import timezone
from airflow.models import Variable
import pendulum
from pip._vendor.distlib.util import Configurator
import datetime as dt
import os,sys,subprocess
from os.path import dirname, join, abspath
import datetime as dt
import re
DBList=[]
Script_Path=[]

c='Common'
root_directory=abspath(join(join(dirname(__file__),'..','..')))
kockpit_path="#ConfiguratorInstallationDrive"

sys.path.insert(0, kockpit_path)
for folders in os.listdir(kockpit_path):
    if os.path.isdir(os.path.join(kockpit_path,folders)):
        if 'DB' in folders:
            DBList.append(folders)
        

DBList=sorted(DBList)
DBList.append(DBList.pop(DBList.index('DB0')))
for d in DBList:
    if 'DB0' in d:
        pass
    else:
        Connection =os.path.join(kockpit_path,d)
        sys.path.insert(0, Connection)
        for folders in os.listdir(Connection):
            if os.path.isdir(os.path.join(Connection,folders)):
                if 'E' in folders: 
                    path=os.path.join(kockpit_path,d,folders,c)
                    Script_Path.append(path)
local_tz = pendulum.timezone("Asia/Calcutta")
dagargs = {
    'owner': 'kockpit',
    'depends_on_past': False,
    'start_date': datetime(2022, 12,20, tzinfo=local_tz),
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
         max_active_runs= 1,
         is_paused_upon_creation=True,
         catchup=False)as dag:
    def Data_Cleaning():
        for i in range(len(Script_Path)):
            Data_Cleaning = BashOperator(
            task_id="Data_Cleaning"+str(i),
            wait_for_downstream=True,
            bash_command=" python3 "+Script_Path[i]+"/DataCleaning.py",
            )
    Data_Cleaning()




