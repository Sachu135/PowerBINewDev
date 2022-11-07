from pyspark.sql.types import *
import datetime
import psycopg2


MnSt = 4
yr = 3
owmode = 'overwrite'
apmode = 'append'
#cdate = datetime.datetime.now()

class PostgresDbInfo:
    Host = "172.16.10.186"      
    Port = "5432"               
    PostgresDB = "kockpit_new"  
    PostgresUrl = "jdbc:postgresql://" + Host + "/" + PostgresDB
    Configurl = "jdbc:postgresql://" + Host + "/Configurator_Linux"
    logsDbUrl = "jdbc:postgresql://" + Host + "/Logs_new"
    url = "jdbc:postgresql://192.10.15.134/"
    props = {"user":"postgres", "password":"sa@123", "driver": "org.postgresql.Driver"}   
conn=psycopg2.connect(dbname = PostgresDbInfo.PostgresDB, user = "postgres", password = "sa@123", host = PostgresDbInfo.Host)
class ConfiguratorDbInfo:
    Host = "20.204.142.98"      #Host IP
    Port = "5432"               #Port
    PostgresDB = "Configurator" 
    Schema = "kockpitdev"              #SchemaName
    PostgresUrl = "jdbc:postgresql://" + Host + "/" + PostgresDB
    props = {"user":"sukriti.saluja@kockpit.in", "password":"$urNX6i5", "driver": "org.postgresql.Driver"}   
conn=psycopg2.connect(dbname = ConfiguratorDbInfo.PostgresDB, user = "sukriti.saluja@kockpit.in", password = "$urNX6i5", host = ConfiguratorDbInfo.Host)
class ConnectionInfo:
    JDBC_PARAM = "jdbc"
    SQL_SERVER_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    SQL_URL="jdbc:sqlserver://{0}:{1};databaseName={2};user={3};password={4}"
    
class PowerBISync:
    LOGIN_URL = 'https://login.microsoftonline.com/common/oauth2/token'
    LOGIN_REQUEST_PARAMS = {'grant_type': 'password',
         'username': 'ankit.kumar@teamcomputerspvtltd.onmicrosoft.com',
         'password': 'Cloud@@1',
         'client_id': 'f2228330-3a89-4050-a936-a366add4b5d4',
         'client_secret': '32z~E.xUXfF-8Ac7.ADqg0tT8bxI~_I~ny',
         'resource': 'https://analysis.windows.net/powerbi/api',
         'prompy': 'admin_consent'}
    WORKSPACE_ID = '941d5847-c431-4f65-94c0-f6e5437b5de1'   #Kockpit_Datamart
    GET_WORKSPACE_DATASET = 'https://api.powerbi.com/v1.0/myorg/groups/' + WORKSPACE_ID + '/datasets'
    REFRESH_WORKSPACE_DATASETS = 'https://api.powerbi.com/v1.0/myorg/groups/' + WORKSPACE_ID + '/datasets/{0}/refreshes'
    REFRESH_NOTIFY_OPTION = {'notifyOption': 'MailOnFailure'}

class Logger: 
    def __init__(self): 
        self._startTime = datetime.datetime.now()
        self._endTimeStr = None
        self._executionTime = None
        self._dateLog = self._startTime.strftime('%Y-%m-%d')
        self._startTimeStr = self._startTime.strftime('%H:%M:%S') 
        self._schema = StructType([
            StructField('Date',StringType(),True),
            StructField('Start_Time',StringType(),True),
            StructField('End_Time', StringType(),True),
            StructField('Run_Time',StringType(),True),
            StructField('File_Name',StringType(),True),
            StructField('DB',StringType(),True),
            StructField('EN', StringType(),True),
            StructField('Status',StringType(),True),
            StructField('Log_Status',StringType(),True),
            StructField('ErrorLineNo.',StringType(),True),
            StructField('Rows',IntegerType(),True),
            StructField('Columns',IntegerType(),True),
            StructField('Source',StringType(),True)
        ])

    def getSchema(self):
        return self._schema
    
    def endExecution(self):
        end_time = datetime.datetime.now()
        self._endTimeStr = end_time.strftime('%H:%M:%S')
        etime = str(end_time-self._startTime)
        self._executionTime = etime.split('.')[0]
    
    def getSuccessLoggedRecord(self, fileName, DBName, EntityName, rowsCount, columnsCount, source):
        return  [{'Date': self._dateLog,
                    'Start_Time': self._startTimeStr,
                    'End_Time': self._endTimeStr,
                    'Run_Time': self._executionTime,
                    'File_Name': fileName,
                    'DB': DBName,
                    'EN': EntityName,
                    'Status': 'Completed',
                    'Log_Status': 'Completed', 
                    'ErrorLineNo.': 'NA', 
                    'Rows': rowsCount, 
                    'Columns': columnsCount,
                    'Source': source
                }]

    def getErrorLoggedRecord(self, fileName, DBName, EntityName, exception, errorLineNo, source):
        return  [{'Date': self._dateLog,
                    'Start_Time': self._startTimeStr,
                    'End_Time': self._endTimeStr,
                    'Run_Time': self._executionTime,
                    'File_Name': fileName,
                    'DB': DBName,
                    'EN': EntityName,
                    'Status': 'Failed',
                    'Log_Status': str(exception),
                    'ErrorLineNo.': str(errorLineNo),
                    'Rows': 0,
                    'Columns': 0,
                    'Source': source
                }]
        
        
    # Customer Segmentation connection strings
    
engine_string = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}".format(
                user = 'postgres',
                password = 'sa@123',
                host = '192.10.15.57',
                port = '5432',
                database = 'kockpit_linux',
                )

config_engine_string = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}".format(
                user = 'postgres',
                password = 'sa@123',
                host = '192.10.15.57',
                port = '5432',
                database = 'Configurator',
                )

connection = psycopg2.connect(user="postgres",
                                  password="sa@123",
                                  host="192.10.15.57",
                                  port="5432",
                                  database="kockpit_linux")