from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from airflow.utils.trigger_rule import TriggerRule
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
DB0_Path=[]
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
        DB0_path=os.path.join(kockpit_path,d,c)
        DB0_Path.append(DB0_path)
        
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
    'depends_on_past': True,
    'start_date': datetime(2022, 7,19, tzinfo=local_tz),
    'email': ['abhishek@kockpit.in'],
    'email_on_failure': True,
    'email_on_retry': True,
    'trigger_rule':TriggerRule.ALL_SUCCESS,
    'wait_for_downstream':True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'max_active_runs': 1,
    'end_date': datetime(2025, 2, 15, tzinfo=local_tz)
}

with DAG('Transformation',
         schedule_interval=None,
         default_args=dagargs,
         max_active_runs= 1,
         is_paused_upon_creation=True,
         catchup=False)as dag:
    def Stage():
        for i in range(len(Script_Path)):
            FULL_Reload = BashOperator(
                task_id="FULL_Reload"+str(i),
                wait_for_downstream=True,
                bash_command=" python3 "+Script_Path[i]+"/transformation.py FULL",
               )
            CONFIGURATOR_Reload = BashOperator(
                task_id="CONFIGURATOR_Reload"+str(i),
                wait_for_downstream=True,
                bash_command=" python3 "+Script_Path[i]+"/transformation.py CONFIGURATOR",
               )
            Masters_Stage2 = BashOperator(
                task_id="Masters_Stage2"+str(i),
                wait_for_downstream=True,
                bash_command=" python3 "+Script_Path[i]+"/transformation.py Masters",
               )
            Sales_Stage2 = BashOperator(
                task_id="Sales_Stage2"+str(i),
                wait_for_downstream=True,
                bash_command=" python3 "+Script_Path[i]+"/transformation.py Sales",
               )
            Purchase_Stage2 = BashOperator(
                task_id="Purchase_Stage2"+str(i),
                wait_for_downstream=True,
                bash_command=" python3 "+Script_Path[i]+"/transformation.py Purchase",
               )
            Finance_Stage2 = BashOperator(
                task_id="Finance_Stage2"+str(i),
                wait_for_downstream=True,
                bash_command=" python3 "+Script_Path[i]+"/transformation.py Finance",
               )
            Inventory_Stage2 = BashOperator(
                task_id="Inventory_Stage2"+str(i),
                wait_for_downstream=True,
                bash_command=" python3 "+Script_Path[i]+"/transformation.py Inventory",
               )
    Stage()
    def DB0():  
          DB0 = BashOperator(
                task_id="DB0",
                wait_for_downstream=True,
                bash_command=" python3 "+DB0_Path[0]+"/transformation.py",
               ) 
    DB0()        
        




