from pyspark.sql import SparkSession,SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import lit,concat
from pyspark.sql.types import *
from os.path import dirname, join, abspath
import os,datetime,sys
import datetime as dt 
from builtins import str
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
STAGE1_PATH=HDFS_PATH+DIR_PATH+"/" +DBName+"/" +EntityName+"/" +"Stage1/ParquetData"
STAGE2_PATH=HDFS_PATH+DIR_PATH+"/" +DBName+"/" +EntityName+"/" +"Stage2/ParquetData"
conf = SparkConf().setMaster(SPARK_MASTER).setAppName("Receivables")\
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
import delta
from delta.tables import *
for dbe in config["DbEntities"]:
    if dbe['ActiveInactive']=='true' and  dbe['Location']==DBEntity:
        CompanyName=dbe['Name']
        CompanyName=CompanyName.replace(" ","")
        try:
            logger = Logger()
            CLE = spark.read.format("delta").load(STAGE1_PATH+"/Cust_ Ledger Entry")
            CLE =CLE.select('DocumentNo_','EntryNo_','DimensionSetID','CustomerPostingGroup','PostingDate')     
            CLE = CLE.withColumnRenamed('DocumentNo_','CLE_Document_No')
            SIH = spark.read.format("delta").load(STAGE1_PATH+"/Sales Invoice Header" )
            SIH = SIH.select('No_','PaymentTermsCode').withColumnRenamed('No_','CLE_Document_No')
            DCLE = spark.read.format("delta").load(STAGE1_PATH+"/Detailed Cust_ Ledg_ Entry" )
            DCLE = DCLE.select('Cust_LedgerEntryNo_','EntryType','DebitAmount')
            CPG = spark.read.format("delta").load(STAGE1_PATH+"/Customer Posting Group")
            CPG = CPG.select('Code','ReceivablesAccount').withColumnRenamed('Code','CustomerPostingGroup')\
                        .withColumnRenamed('ReceivablesAccount','GLAccount')
            DSE=spark.read.format("delta").load(STAGE2_PATH+"/"+"Masters/DSE")
            cond = [CLE["EntryNo_"] == DCLE["Cust_LedgerEntryNo_"]]
            finalDF = DCLE.join(CLE, cond, 'left')
            finalDF = finalDF.join(SIH, 'CLE_Document_No', 'left')
            finalDF = finalDF.join(CPG,'CustomerPostingGroup','left')
            finalDF = finalDF.join(DSE,"DimensionSetID",'left').drop("DimensionSetID")
            finalDF = Kockpit.RenameDuplicateColumns(finalDF)
            finalDF=finalDF.withColumn("DBName",concat(lit(DBName))).withColumn("EntityName",concat(lit(EntityName)))
            finalDF = finalDF.withColumn('LinkDateKey',concat(finalDF["DBName"],lit('|'),finalDF["EntityName"],lit('|'),finalDF["PostingDate"]))
            finalDF = Kockpit.RenameDuplicateColumns(finalDF)
            finalDF.coalesce(1).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(STAGE2_PATH+"/"+"Sales/Receivables")
            logger.endExecution()
            
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"
            
            log_dict = logger.getSuccessLoggedRecord("Sales.Receivables", DBName, EntityName, finalDF.count(), len(finalDF.columns), IDEorBatch)
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
            os.system("spark-submit "+Kockpit_Path+"/Email.py 1 Receivables '"+CompanyName+"' "+DBEntity+" "+str(exc_traceback.tb_lineno)+"")
            log_dict = logger.getErrorLoggedRecord('Sales.Receivables', DBName, EntityName, str(ex), str(exc_traceback.tb_lineno), IDEorBatch)
            log_df = spark.createDataFrame(log_dict, logger.getSchema())
            log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)
print('sales_Receivables completed: ' + str((dt.datetime.now()-st).total_seconds()))