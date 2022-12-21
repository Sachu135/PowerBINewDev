from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession,Row
from pyspark.sql import functions as f
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import regexp_replace, col, udf, broadcast
import datetime
import datetime as dt
import multiprocessing
import threading, queue
from threading import Lock
from multiprocessing.pool import ThreadPool
import urllib.request
import os,sys
from os.path import dirname, join, abspath
Kockpit_Path =abspath(join(join(dirname(__file__),'..','..')))
DB_path =abspath(join(join(dirname(__file__),'..')))
sys.path.insert(0,'../../')
sys.path.insert(0, DB_path)
root_directory =abspath(join(join(dirname(__file__),'..','..')))
root_directory=root_directory+"/"
DBList=[]
for folders in os.listdir(root_directory):
    if os.path.isdir(os.path.join(root_directory,folders)):
        if 'DB' in folders:
            if 'DB0' in folders:
                pass
            else:
                DBList.insert(0,folders )
Connection =abspath(join(join(dirname(__file__), '..'),'..',DBList[0]))
sys.path.insert(0, Connection)
from Configuration.Constant import *
from Configuration.udf import *
from Configuration import udf as Kockpit
from pyspark.sql import functions as F
import pandas as pd
import os,sys,subprocess
from os.path import dirname, join, abspath
import json
import urllib3
def PowerBI_Purchase_Refresh():
    argv = sys.argv[1:]
    http = urllib3.PoolManager()
    loginReq = http.request('POST', PowerBISync.LOGIN_URL, fields=PowerBISync.LOGIN_REQUEST_PARAMS)
    loginData = json.loads(loginReq.data.decode('utf-8'))
    access_token = loginData['access_token']
    
    auth_header_val = 'Bearer ' + access_token
    auth_header = {'authorization' : auth_header_val}
    
    datasetsReq = http.request('GET', PowerBISync.GET_WORKSPACE_DATASET, headers=auth_header)
    dataSetsResult = json.loads(datasetsReq.data.decode('utf-8'))
    
    encoded_data = json.dumps(PowerBISync.REFRESH_NOTIFY_OPTION).encode('utf-8')
    auth_header = {'authorization' : auth_header_val, 'Content-Type': 'application/json'}
    
    for dataset in dataSetsResult['value']:
        if (dataset['name']==PowerBISync.Purchase_Dataset) :
            print("Refresh Started for - ",str(dataset['name']))
            refresh_url = PowerBISync.REFRESH_WORKSPACE_DATASETS.format(dataset['id'])
            if (len(argv) > 0 and dataset['id'] in argv) or (len(argv) == 0):
                dsRefreshReq = http.request('POST', refresh_url, headers=auth_header, body=encoded_data)
                resp_body = dsRefreshReq.data.decode('utf-8')
                if (dsRefreshReq.status != 202):
                    raise Exception(resp_body)
if __name__ == "__main__":
    PowerBI_Purchase_Refresh()    
