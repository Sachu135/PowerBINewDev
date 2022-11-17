from pyspark.sql import SparkSession,SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import *
import pyspark.sql.functions as F
import os,sys
from os.path import dirname, join, abspath
import datetime as dt
root_directory =abspath(join(join(dirname(__file__), '..'),'..','..','..',))
root_directory=root_directory+"/"
DBList=[]
for folders in os.listdir(root_directory):
    if os.path.isdir(os.path.join(root_directory,folders)):
        if 'DB' in folders:
            if '0' in folders:
                pass
            else:
                DBList.append(folders )
Connection =abspath(join(join(dirname(__file__), '..'),'..','..','..',DBList[0]))
sys.path.insert(0, Connection)
from Configuration.Constant import *
Abs_Path =abspath(join(join(dirname(__file__), '..'),'..','..','..')) 
Kockpit_Path =abspath(join(join(dirname(__file__), '..'),'..','..','..'))
DBO_Path=abspath(join(join(dirname(__file__), '..'),'..','..'))
DB0 =os.path.split(DBO_Path)
DB0 = DB0[1]
owmode = 'overwrite'
apmode = 'append'                           
st = dt.datetime.now()
conf = SparkConf().setMaster(SPARK_MASTER).setAppName("Customer")\
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
        .set("spark.kryoserializer.buffer.max","512m")\
        .set("spark.cores.max","24")\
        .set("spark.executor.memory","8g")\
        .set("spark.driver.memory","30g")\
        .set("spark.driver.maxResultSize","0")\
        .set("spark.sql.debug.maxToStringFields","500")\
        .set("spark.driver.maxResultSize","20g")\
        .set("spark.memory.offHeap.enabled",'true')\
        .set("spark.memory.offHeap.size","100g")\
        .set('spark.scheduler.mode', 'FAIR')\
        .set("spark.sql.broadcastTimeout", "36000")\
        .set("spark.network.timeout", 10000000)\
        .set("spark.sql.codegen.wholeStage","false")\
        .set("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0")\
        .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
        .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
        .set("spark.databricks.delta.vacuum.parallelDelete.enabled",'true')\
        .set("spark.databricks.delta.retentionDurationCheck.enabled",'false')\
        .set('spark.hadoop.mapreduce.output.fileoutputformat.compress', 'false')\
        .set("spark.rapids.sql.enabled", True)\
        .set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
sc = SparkContext(conf = conf)
sqlCtx = SQLContext(sc)
spark = sqlCtx.sparkSession
fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
ConfTab='tblCompanyName'
try:
    Query="(SELECT *\
                    FROM "+ConfiguratorDbInfo.Schema+"."+chr(34)+ConfTab+chr(34)+") AS df"
    CompanyDetail = spark.read.format("jdbc").options(url=ConfiguratorDbInfo.PostgresUrl, dbtable=Query,user=ConfiguratorDbInfo.props["user"],password=ConfiguratorDbInfo.props["password"],driver= ConfiguratorDbInfo.props["driver"]).load()
    CompanyDetail=CompanyDetail.filter((CompanyDetail['ActiveCompany']=='true'))
    for d in range(len(DBList)):  
        DB=DBList[d]
        logger =Logger()
        Query="(SELECT *\
                    FROM "+ConfiguratorDbInfo.Schema+"."+chr(34)+ConfTab+chr(34)+") AS df"
        CompanyDetail = spark.read.format("jdbc").options(url=ConfiguratorDbInfo.PostgresUrl, dbtable=Query,user=ConfiguratorDbInfo.props["user"],password=ConfiguratorDbInfo.props["password"],driver= ConfiguratorDbInfo.props["driver"]).load()
        CompanyDetail=CompanyDetail.filter((CompanyDetail['ActiveCompany']=='true'))
        CompanyDetail=CompanyDetail.filter((CompanyDetail['DBName']==DB))
        NoofRows = CompanyDetail.count()  
        
        for i in range(NoofRows): 
            
                DBName=(CompanyDetail.collect()[i]['DBName'])
                EntityName =(CompanyDetail.collect()[i]['NewCompanyName'])
                CompanyName=(CompanyDetail.collect()[i]['CompanyName'])
                DBE=DBName+EntityName
                CompanyName=CompanyName.replace(" ","")
                Path = HDFS_PATH+DIR_PATH+"/"+DBName+"/"+EntityName+"/Stage2/ParquetData/Masters/Customer"
                fe = fs.exists(sc._jvm.org.apache.hadoop.fs.Path(Path))
                if(fe):
                    finalDF=spark.read.format("delta").load(Path)
                    if (d==0) & (i==0):
                        finalDF=finalDF1
                        
                    else:
                        finalDF=finalDF.unionByName(finalDF1,allowMissingColumns=True)
                        
                else:
                    print("Customer "+DBName+EntityName+" Does not exist")
    finalDF.write.jdbc(url=PostgresDbInfo.PostgresUrl , table="Masters.Customer", mode=owmode, properties=PostgresDbInfo.props)
    logger.endExecution()
    try:
        IDEorBatch = sys.argv[1]
    except Exception as e :
        IDEorBatch = "IDLE"

    log_dict = logger.getSuccessLoggedRecord("Masters.Customer", DB0, "", finalDF.count(), len(finalDF.columns), IDEorBatch)
    log_df = spark.createDataFrame(log_dict, logger.getSchema())
    log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)                 
except Exception as ex:
    exc_type,exc_value,exc_traceback=sys.exc_info()
    print("Error:",ex)
    print("type - "+str(exc_type))
    print("File - "+exc_traceback.tb_frame.f_code.co_filename)
    print("Error Line No. - "+str(exc_traceback.tb_lineno))
    ex = str(ex)
    logger.endExecution()
    try:
        IDEorBatch = sys.argv[1]
    except Exception as e :
        IDEorBatch = "IDLE"
    DBE=DBName+EntityName
    os.system("spark-submit "+Kockpit_Path+"\Email.py 1 Customer "+CompanyName+" "" "+str(exc_traceback.tb_lineno)+"")   
    log_dict = logger.getErrorLoggedRecord('Masters.Customer', DB0, "" , str(ex), exc_traceback.tb_lineno, IDEorBatch)
    log_df = spark.createDataFrame(log_dict, logger.getSchema())
    log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)        
print('masters Customer completed: ' + str((dt.datetime.now()-st).total_seconds()))        