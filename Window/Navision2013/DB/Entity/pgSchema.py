
import psycopg2
import pandas as pd
from datetime import timedelta
from datetime import datetime
import datetime as dt
import psycopg2, sys
from sqlalchemy import create_engine
import os
import os,sys
from os.path import dirname, join, abspath
Connection =abspath(join(join(dirname(__file__),'..')))
sys.path.insert(0, Connection)
from Configuration.AppConfig import * 
from Configuration.Constant import *
from Configuration.udf import *

Stage2_Path =os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    conn=psycopg2.connect(dbname = PostgresDbInfo.PostgresDB, user = PostgresDbInfo.props["user"], password = PostgresDbInfo.props["password"], host = PostgresDbInfo.Host)
    conn.autocommit = True
    cursor = conn.cursor()
    # cursor.execute("CREATE DATABASE "PostgresDbInfo.PostgresDB";")
    # cursor.execute("CREATE SCHEMA masters AUTHORIZATION postgres;")
    # cursor.execute("CREATE SCHEMA sales AUTHORIZATION postgres;")
    # cursor.execute("CREATE SCHEMA purchase AUTHORIZATION postgres;")
    # cursor.execute("CREATE SCHEMA finance AUTHORIZATION postgres;")
    cursor.execute("CREATE SCHEMA inventory AUTHORIZATION postgres;")
    #cursor.execute("CREATE SCHEMA logs AUTHORIZATION postgres;")
    cursor.execute("select * from information_schema.schemata;")
    print(cursor.fetchall())
except Exception as e:
    print(e)
    flag = 1
    print(flag)
    print("connection error")
