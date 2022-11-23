
from pyspark.sql import SparkSession,SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import lit, year,when,to_date,concat
from pyspark.sql.types import *
import os,sys,datetime,time,traceback
from os.path import dirname,join, abspath
import datetime as dt 
from builtins import str
from datetime import date
st = dt.datetime.now()
Kockpit_Path =abspath(join(join(dirname(__file__),'..','..','..','..','..')))
DB_path =abspath(join(join(dirname(__file__),'..','..','..','..')))
sys.path.insert(0,'../../')
sys.path.insert(0, DB_path)
from Configuration.AppConfig import * 
from Configuration.Constant import *
from Configuration.udf import *
from Configuration import udf as Kockpit

Filepath = os.path.dirname(os.path.abspath(__file__))
FilePathSplit = Filepath.split('/')
DBName = FilePathSplit[-5]
EntityName = FilePathSplit[-4]
DBEntity = DBName+EntityName
entityLocation = DBName+EntityName
STAGE1_PATH=HDFS_PATH+DIR_PATH+"/" +DBName+"/" +EntityName+"/" +"Stage1/ParquetData"
STAGE2_PATH=HDFS_PATH+DIR_PATH+"/" +DBName+"/" +EntityName+"/" +"Stage2/ParquetData"
conf = SparkConf().setMaster(SPARK_MASTER).setAppName("Budget")\
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
import delta
from delta.tables import *
fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
cdate = datetime.datetime.now().strftime('%Y-%m-%d')
for dbe in config["DbEntities"]:
    if dbe['ActiveInactive']=='true' and  dbe['Location']==DBEntity:
        CompanyName=dbe['Name']
        CompanyName=CompanyName.replace(" ","")
        try:
            logger = Logger()
            GLB = spark.read.format("delta").load(STAGE1_PATH+"/G_L Budget Entry")       
            GLB=GLB.withColumnRenamed("G_LAccountNo_","GLAccount")
            GLB = GLB.withColumn("Description", when(GLB.Description == 'RE Salary','9999999').otherwise(GLB.GLAccount))
            GLB =GLB.filter(year(GLB['Date'])!=1753)
            GLB=GLB.filter(GLB['BudgetName'].like('SALESTGT%'))
            GLB=GLB.select("GLAccount","Date","Amount","Description","DimensionSetID")
            GLB=GLB.groupBy("GLAccount","Date","DimensionSetID").sum("Amount")
            GLB=GLB.withColumnRenamed("sum(Amount)","Amount")  
            GLB=GLB.withColumn("LinkDate",to_date(GLB.Date))  
            GLB=GLB.withColumn("DBName",concat(lit(DBName))).withColumn("EntityName",concat(lit(EntityName)))    
            GLB = GLB.withColumn('Link_GLAccount_Key',concat(GLB["DBName"],lit('|'),GLB["EntityName"],lit('|'),GLB["GLAccount"]))\
                             .withColumn('LinkDateKey',concat(GLB["DBName"],lit('|'),GLB["EntityName"],lit('|'),GLB["LinkDate"])).drop("Date")
            DSE=spark.read.format("delta").load(STAGE2_PATH+"/"+"Masters/DSE").drop("DBName","EntityName").drop("Link_CUSTOMER","Link_CUSTOMERKey","Link_EMPLOYEE","Link_EMPLOYEEKey","Link_BRANCH","Link_BRANCHKey","Link_TARGETPROD","Link_TARGETPRODKey","Link_OTBRANCH","Link_OTBRANCHKey","Link_PRODUCT","Link_PRODUCTKey","Link_SALESPER","Link_SALESPERKey","Link_VENDOR","Link_VENDORKey","Link_PROJECT","Link_PROJECTKey")
            GLB = GLB.join(DSE,"DimensionSetID",'left')  
            GLB.coalesce(1).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(STAGE2_PATH+"/"+"Finance/Budget")
             
            logger.endExecution()
            
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"
            log_dict = logger.getSuccessLoggedRecord("Finance.Budget", DBName, EntityName, GLB.count(), len(GLB.columns), IDEorBatch)
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
            os.system("spark-submit "+Kockpit_Path+"/Email.py 1 Budget '"+CompanyName+"' "+DBEntity+" "+str(exc_traceback.tb_lineno)+" ")
    
            log_dict = logger.getErrorLoggedRecord('Finance.Budget', DBName, EntityName, str(ex), str(exc_traceback.tb_lineno), IDEorBatch)
            log_df = spark.createDataFrame(log_dict, logger.getSchema())
            log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)
print('Finance_Budget completed: ' + str((dt.datetime.now()-st).total_seconds()))

             