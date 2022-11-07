from pyspark.sql import SparkSession,SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import lit, year,when,datediff,col,to_date,concat
from pyspark.sql.types import *
from os.path import dirname, join, abspath
import re,os,datetime,sys
import datetime as dt
from builtins import str
from datetime import date
import pyspark.sql.functions as F

st = dt.datetime.now()
Kockpit_Path =abspath(join(join(dirname(__file__),'..','..','..','..','..')))
DB1_path =abspath(join(join(dirname(__file__),'..','..','..','..')))
sys.path.insert(0,'../../')
sys.path.insert(0, DB1_path)
from Configuration.AppConfig import * 
from Configuration.Constant import *
from Configuration.udf import *
from Configuration import udf as Kockpit
from pyspark.sql.types import Row

Filepath = os.path.dirname(os.path.abspath(__file__))
FilePathSplit = Filepath.split('/')
DBName = FilePathSplit[-5]
EntityName = FilePathSplit[-4]
DBEntity = DBName+EntityName

DBNamepath= abspath(join(join(dirname(__file__), '..'),'..','..','..'))
STAGE1_Configurator_Path=Kockpit_Path+"/" +DBName+"/" +EntityName+"/" +"Stage1/ConfiguratorData/"
STAGE1_PATH=Kockpit_Path+"/" +DBName+"/" +EntityName+"/" +"Stage1/ParquetData"
STAGE2_PATH=Kockpit_Path+"/" +DBName+"/" +EntityName+"/" +"Stage2/ParquetData"

conf = SparkConf().setMaster(SPARK_MASTER).setAppName("SalesOrder")\
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
        .set("spark.kryoserializer.buffer.max","512m")\
        .set("spark.cores.max","24")\
        .set("spark.executor.memory","8g")\
        .set("spark.driver.memory","24g")\
        .set("spark.driver.maxResultSize","20g")\
        .set("spark.memory.offHeap.enabled",'true')\
        .set("spark.memory.offHeap.size","100g")\
        .set('spark.scheduler.mode', 'FAIR')\
        .set("spark.sql.broadcastTimeout", "36000")\
        .set("spark.network.timeout", 10000000)\
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

for dbe in config["DbEntities"]:
    if dbe['ActiveInactive']=='true' and  dbe['Location']==DBEntity:
        CompanyName=dbe['Name']
        CompanyName=CompanyName.replace(" ","")
        try: 
            logger = Logger()
            SH =  spark.read.format("delta").load(STAGE1_PATH+"/Sales Header")
            SH=SH.select("DocumentType","No_","ShipmentDate","PostingDate","CurrencyFactor","PromisedDeliveryDate","Cust_OrderRec_Date")  
            SL =spark.read.format("delta").load(STAGE1_PATH+"/Sales Line").drop("PromisedDeliveryDate")
            SL=SL.select("DocumentNo_","DimensionSetID","No_","OutstandingQuantity","Amount","LineAmount")
            SL = SL.withColumnRenamed('No_','ItemNo_')
            SO = SL.join(SH, SL['DocumentNo_']==SH['No_'], 'left')
            SO = SO.filter(SO['DocumentType']==1).filter(year(SO['PostingDate'])!=1753)
            SO = SO.withColumn("NOD_Promised_Date",datediff(SO['PromisedDeliveryDate'],lit(datetime.datetime.today())))
            SO =  SO.withColumn("LineAmount",when((SO.LineAmount/SO.CurrencyFactor).isNull(),SO.LineAmount).otherwise(SO.LineAmount/SO.CurrencyFactor))\
                    .withColumn("Transaction_Type",lit("SalesOrder"))
              
            PDDBucket = spark.read.format("delta").load(STAGE1_Configurator_Path+"/tblPDDBucket")
            
            DSE=spark.read.format("delta").load(STAGE2_PATH+"/"+"Masters/DSE").drop("DBName","EntityName")
            Maxoflt = PDDBucket.filter(PDDBucket['BucketName']=='<')
            MaxLimit = int(Maxoflt.select('MaxLimit').first()[0])
            Minofgt = PDDBucket.filter(PDDBucket['BucketName']=='>')
            MinLimit = int(Minofgt.select('MinLimit').first()[0])
            SO = SO.join(PDDBucket,SO.NOD_Promised_Date == PDDBucket.Nod,'left').drop('ID','UpperLimit','LowerLimit')
            SO=SO.withColumn('BucketName',when(SO.NOD_Promised_Date>=MinLimit,lit(str(MinLimit)+'+')).otherwise(SO.BucketName))\
                        .withColumn('Nod',when(SO.NOD_Promised_Date>=MinLimit,SO.NOD_Promised_Date).otherwise(SO.Nod))
            SO=SO.withColumn('BucketName',when(SO.NOD_Promised_Date<=(MaxLimit),lit("Not Due")).otherwise(SO.BucketName))\
                        .withColumn('Nod',when(SO.NOD_Promised_Date<=(MaxLimit), SO.NOD_Promised_Date).otherwise(SO.Nod))
            finalDF = SO.join(DSE,"DimensionSetID",'left')
            finalDF=finalDF.withColumn("PostingDate",to_date(col("PostingDate")))
            finalDF = finalDF.withColumn('LinkItemKey',concat(finalDF["DBName"],lit('|'),finalDF["EntityName"],lit('|'),finalDF["ItemNo_"]))\
                        .withColumn('LinkDateKey',concat(finalDF["DBName"],lit('|'),finalDF["EntityName"],lit('|'),finalDF["PostingDate"])).drop('ItemNo_')
            finalDF = finalDF.select([F.col(col).alias(col.replace(" ","")) for col in finalDF.columns])
            finalDF.coalesce(1).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(STAGE2_PATH+"/"+"Sales/SalesOrder")

            logger.endExecution()
        
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"
            log_dict = logger.getSuccessLoggedRecord("Sales.Sales_Order", DBName, EntityName, finalDF.count(), len(finalDF.columns), IDEorBatch)
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
            os.system("spark-submit "+Kockpit_Path+"/Email.py 1 SalesOrder '"+CompanyName+"' "+DBEntity+" "+str(exc_traceback.tb_lineno)+"")
            
            log_dict = logger.getErrorLoggedRecord('Sales.Sales_Order', DBName, EntityName, str(ex), str(exc_traceback.tb_lineno), IDEorBatch)
            log_df = spark.createDataFrame(log_dict, logger.getSchema())
            log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)
print('sales_SalesOrder completed: ' + str((dt.datetime.now()-st).total_seconds()))