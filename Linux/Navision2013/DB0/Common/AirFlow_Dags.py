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
EntList=[]
path_f=[]
list_f=[]
dir_list=[]
c='Common'
root_directory=abspath(join(join(dirname(__file__),'..','..')))
kockpit_path="/home/hadoop/KOCKPIT"

sys.path.insert(0, kockpit_path)
for folders in os.listdir(kockpit_path):
    if os.path.isdir(os.path.join(kockpit_path,folders)):
        if 'DB' in folders:
            DBList.append(folders)
        

DBList=sorted(DBList)
DBList.append(DBList.pop(DBList.index('DB0')))
for d in DBList:
    if 'DB0' in d:
        path=os.path.join(kockpit_path,d,c)
        path_f.append(path)
        
    else:
        Connection =os.path.join(kockpit_path,d)
        sys.path.insert(0, Connection)
        for folders in os.listdir(Connection):
            if os.path.isdir(os.path.join(Connection,folders)):
                if 'E' in folders: 
                    path=os.path.join(kockpit_path,d,folders,c)
                    path_f.append(path)

local_tz = pendulum.timezone("Asia/Calcutta")
dagargs = {
    'owner': 'kockpit',
    'depends_on_past': False,
    'start_date': datetime(2022, 7,19, tzinfo=local_tz),
    'email': ['amit.kumar@kockpit.in'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'end_date': datetime(2025, 2, 15, tzinfo=local_tz)
}


dag = DAG('Ingestion', default_args=dagargs, catchup=False,schedule_interval="0 2 * * *")
for i in range(len(path_f)):
    DataIngestion = BashOperator(
        task_id="ingestion"+str(i),
        bash_command=" python3 "+path_f[i]+"/transformation.py",
        dag=dag)



