from pyspark.sql import SparkSession,SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import *
import pyspark.sql.functions as F
import os,sys
from os.path import dirname, join, abspath
import datetime as dt
begin_time = dt.datetime.now()
Connection =abspath(join(join(dirname(__file__), '..'),'..','..','..','DB1'))
Stage1_Path = abspath(join(join(dirname(__file__),'..','..','..')))
sys.path.insert(0, Connection)
from Configuration.AppConfig import * 
from Configuration.Constant import *
from Configuration.udf import *
Connection =abspath(join(join(dirname(__file__), '..'),'..','..','..','DB1','E1'))
Abs_Path =abspath(join(join(dirname(__file__), '..'),'..','..','..')) 
KockpitPath =abspath(join(join(dirname(__file__), '..'),'..','..','..'))
DB0=abspath(join(join(dirname(__file__), '..'),'..','..'))
DB0 =os.path.split(DB0)
DB0 = DB0[1]
owmode = 'overwrite'
apmode = 'append'                           
st = dt.datetime.now()
conf = SparkConf().setMaster("local[*]").setAppName("Vendor").\
                    set("spark.sql.shuffle.partitions",16).\
                    set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").\
                    set("spark.local.dir", "/tmp/spark-temp").\
                    set("spark.driver.memory","30g").\
                    set("spark.executor.memory","30g").\
                    set("spark.driver.cores",'*').\
                    set("spark.driver.maxResultSize","0").\
                    set("spark.sql.debug.maxToStringFields", "1000").\
                    set("spark.executor.instances", "20").\
                    set('spark.scheduler.mode', 'FAIR').\
                    set("spark.sql.broadcastTimeout", "36000").\
                    set("spark.network.timeout", 10000000).\
                    set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY").\
                    set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "LEGACY").\
                    set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED").\
                    set("spark.sql.legacy.timeParserPolicy","LEGACY").\
                    set("spark.sql.legacy.parquet.int96RebaseModeInWrite","LEGACY").\
                    set("spark.sql.legacy.parquet.int96RebaseModeInWrite","CORRECTED")
sc = SparkContext(conf = conf)
sqlCtx = SQLContext(sc)
spark = sqlCtx.sparkSession 
fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration()) 
i=1
for dbe in config["DbEntities"]:
    try:
        logger =Logger()
        count=len(config["DbEntities"])
        if dbe['ActiveInactive']=='true' and dbe==config["DbEntities"][0]:
            try:
                location=dbe['Location']
                DBName=location[0:3]
                EntityName=location[-2:]
                CompanyName=dbe['Location']+dbe['Name']
                CompanyName=CompanyName.replace(" ","")
                Path = KockpitPath+"/"+DBName+"/"+EntityName+"/Stage2/ParquetData/Masters/Vendor"
                fe = fs.exists(sc._jvm.org.apache.hadoop.fs.Path(Path))
                if(fe):
                    finalDF=spark.read.format("parquet").load(Path)
                    finalDF.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="Masters.Vendor", mode=owmode, properties=PostgresDbInfo.props)
            except Exception as ex:
                exc_type,exc_value,exc_traceback=sys.exc_info()
                print("Error:",ex)
                print("type - "+str(exc_type))
                print("File - "+exc_traceback.tb_frame.f_code.co_filename)
                print("Error Line No. - "+str(exc_traceback.tb_lineno))
                logger.endExecution()
                try:
                    IDEorBatch = sys.argv[1]
                except Exception as e :
                    IDEorBatch = "IDLE"
                os.system("spark-submit "+KockpitPath+"/Email.py 1 Vendor "+CompanyName+" "+DB0+" "+str(exc_traceback.tb_lineno)+"")
                log_dict = logger.getErrorLoggedRecord('Masters.Vendor  ', DB0, " ", str(ex), str(exc_traceback.tb_lineno), IDEorBatch)
                log_df = spark.createDataFrame(log_dict, logger.getSchema())
                log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)        
        elif dbe['ActiveInactive']=='true' and dbe==config["DbEntities"][i]:
            try:
                location=dbe['Location']
                DBName=location[0:3] 
                EntityName=location[-2:]
                CompanyName=dbe['Location']+dbe['Name']
                CompanyName=CompanyName.replace(" ","")
                Path1 =KockpitPath+"/"+DBName+"/"+EntityName+"/Stage2/ParquetData/Masters/Vendor"
                fe = fs.exists(sc._jvm.org.apache.hadoop.fs.Path(Path1))
                if(fe):
                    finalDF=spark.read.format("parquet").load(Path1)
                    finalDF.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="Masters.Vendor", mode=apmode, properties=PostgresDbInfo.props)
            except Exception as ex:
                exc_type,exc_value,exc_traceback=sys.exc_info()
                print("Error:",ex)
                print("type - "+str(exc_type))
                print("File - "+exc_traceback.tb_frame.f_code.co_filename)
                print("Error Line No. - "+str(exc_traceback.tb_lineno))
                logger.endExecution()
            
                try:
                    IDEorBatch = sys.argv[1]
                except Exception as e :
                    IDEorBatch = "IDLE"
                
                os.system("spark-submit "+KockpitPath+"/Email.py 1 Vendor "+CompanyName+" "+DB0+" "+str(exc_traceback.tb_lineno)+"")
                log_dict = logger.getErrorLoggedRecord('Masters.Vendor  ', DB0, " ", str(ex), str(exc_traceback.tb_lineno), IDEorBatch)
                log_df = spark.createDataFrame(log_dict, logger.getSchema())
                log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)                  
        elif dbe['ActiveInactive']=='false':
            
            continue               
    except Exception as ex:
                exc_type,exc_value,exc_traceback=sys.exc_info()
                print("Error:",ex)
                print("type - "+str(exc_type))
                print("File - "+exc_traceback.tb_frame.f_code.co_filename)
                print("Error Line No. - "+str(exc_traceback.tb_lineno))
                logger.endExecution()
                try:
                    IDEorBatch = sys.argv[1]
                except Exception as e :
                    IDEorBatch = "IDLE"
                os.system("spark-submit "+KockpitPath+"/Email.py 1 Vendor "+CompanyName+" "+DB0+" "+str(exc_traceback.tb_lineno)+"")
                log_dict = logger.getErrorLoggedRecord('Masters.Vendor  ', DB0, " ", str(ex), str(exc_traceback.tb_lineno), IDEorBatch)
                log_df = spark.createDataFrame(log_dict, logger.getSchema())
                log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)
i+=1
print('Masters_Vendor DB0 completed: ' + str((dt.datetime.now()-st).total_seconds()))  
