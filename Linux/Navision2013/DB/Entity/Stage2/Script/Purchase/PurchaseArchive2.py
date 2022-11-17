from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import *
from datetime import date
from pyspark.sql.functions import col,concat_ws,year,when,to_date,lit,datediff
import datetime as dt
from pyspark.sql.window import Window
from pyspark.sql.functions import col,avg,sum,min,max,row_number
import pyspark.sql.functions as f
import os,sys
from os.path import dirname, join, abspath
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
STAGE1_Configurator_Path=HDFS_PATH+DIR_PATH+"/" +DBName+"/" +EntityName+"/" +"Stage1/ConfiguratorData/"
conf = SparkConf().setMaster(SPARK_MASTER).setAppName("PurchaseArchive2")\
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
for dbe in config["DbEntities"]:
    if dbe['ActiveInactive']=='true' and  dbe['Location']==DBEntity:
        CompanyName=dbe['Name']
        CompanyName=CompanyName.replace(" ","")
        try:
            
            logger = Logger()
            entityLocation = DBName+EntityName
            pah = spark.read.format("delta").load(STAGE1_PATH+"/Purchase Header Archive").drop("PromisedReceiptDate")
            DSE=spark.read.format("delta").load(STAGE2_PATH+"/"+"Masters/DSE").drop("DBName","EntityName")
            ph=spark.read.format("delta").load(STAGE1_PATH+"/Purchase Header")
            ph=ph.withColumn("PromisedReceiptDate",when((year(ph.PromisedReceiptDate)<1900)|(ph.PromisedReceiptDate.isNull()),to_date(ph.PostingDate)).otherwise(to_date(ph.PromisedReceiptDate)))
            ph=ph.select("No_","DimensionSetID","PromisedReceiptDate")
            ph=ph.withColumn("Flag",lit("Open"))
            w = Window.partitionBy('No_')
            pah=pah.withColumn('Version_No_max', f.max('VersionNo_').over(w))\
                                    .where(f.col('VersionNo_') == f.col('Version_No_max')).drop("Version_No_max")
            pah=pah.withColumn('LinkVersionKey',concat_ws('|',pah.No_,pah.VersionNo_))
            pah = pah.filter((year(col("PostingDate"))!='1753'))
            pal = spark.read.format("delta").load(STAGE1_PATH+"/Purchase Line Archive").drop("DBName","EntityName","PostingDate","PromisedReceiptDate")
            w1 = Window.partitionBy('DocumentNo_')
            pal=pal.withColumn('Version_No_max', f.max('VersionNo_').over(w1))\
                                    .where(f.col('VersionNo_') == f.col('Version_No_max')).drop("Version_No_max")
            pal=pal.withColumn('LinkVersionKey',concat_ws('|',pal.DocumentNo_,pal.VersionNo_))
            cond = [pal.LinkVersionKey == pah.LinkVersionKey]
            Purchase = Kockpit.LJOIN(pal,pah,cond)
            cond2 = [Purchase.DocumentNo_ == ph.No_]
            Purchase = Kockpit.LJOIN(Purchase,ph,cond2)
            Purchase= Purchase.withColumn("NODays",datediff(Purchase['PromisedReceiptDate'],lit(datetime.datetime.today())))
            PDDBucket =spark.read.format("delta").load(STAGE1_Configurator_Path+"/tblPDDBucket")
            Maxoflt = PDDBucket.filter(PDDBucket['BucketName']=='<')
            MaxLimit = int(Maxoflt.select('MaxLimit').first()[0])
            Minofgt = PDDBucket.filter(PDDBucket['BucketName']=='>')
            MinLimit = int(Minofgt.select('MinLimit').first()[0])
            Purchase = Purchase.join(PDDBucket,Purchase.NODays == PDDBucket.Nod,'left').drop('ID','UpperLimit','LowerLimit')
            Purchase=Purchase.withColumn('BucketName',when(Purchase.NODays>=MinLimit,lit(str(MinLimit)+'+')).otherwise(Purchase.BucketName))\
                        .withColumn('Nod',when(Purchase.NODays>=MinLimit,Purchase.NODays).otherwise(Purchase.Nod))
            Purchase=Purchase.withColumn('BucketName',when(Purchase.NODays<=(MaxLimit),lit("Not Due")).otherwise(Purchase.BucketName))\
                        .withColumn('Nod',when(Purchase.NODays<=(MaxLimit), Purchase.NODays).otherwise(Purchase.Nod))
            Purchase = Purchase.join(DSE,"DimensionSetID",'left')
            Purchase = Kockpit.RenameDuplicateColumns(Purchase)
            Purchase.coalesce(1).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(STAGE2_PATH+"/"+"Purchase/PurchaseArchive2")
                  
            logger.endExecution()
             
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"
            log_dict = logger.getSuccessLoggedRecord("Purchase.PurchaseArchive2", DBName, EntityName, Purchase.count(), len(Purchase.columns), IDEorBatch)
            log_df = spark.createDataFrame(log_dict, logger.getSchema())
            log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)
            
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
            os.system("spark-submit "+Kockpit_Path+"/Email.py 1 PurchaseArchive2 '"+CompanyName+"' "+DBEntity+" "+str(exc_traceback.tb_lineno)+" ")
                    
            log_dict = logger.getErrorLoggedRecord('Purchase.PurchaseArchive2', DBName, EntityName, str(ex), str(exc_traceback.tb_lineno), IDEorBatch)
            log_df = spark.createDataFrame(log_dict, logger.getSchema())
            log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)
print('purchase_PurchaseArchive2 completed: ' + str((dt.datetime.now()-st).total_seconds()))

