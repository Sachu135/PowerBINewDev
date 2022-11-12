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
FilePathSplit = Filepath.split('\\')
DBName = FilePathSplit[-5]
EntityName = FilePathSplit[-4]
DBEntity = DBName+EntityName
entityLocation = DBName+EntityName
STAGE1_PATH=Kockpit_Path+"/" +DBName+"/" +EntityName+"/" +"Stage1/ParquetData"
STAGE2_PATH=Kockpit_Path+"/" +DBName+"/" +EntityName+"/" +"Stage2/ParquetData"
STAGE1_Configurator_Path=Kockpit_Path+"/" +DBName+"/" +EntityName+"/" +"Stage1/ConfiguratorData/"
conf = SparkConf().setMaster("local[16]").setAppName("PurchaseArchive2").\
                    set("spark.sql.shuffle.partitions",16).\
                    set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").\
                    set("spark.local.dir", "/tmp/spark-temp").\
                    set("spark.driver.memory","30g").\
                    set("spark.executor.memory","30g").\
                    set("spark.driver.cores",16).\
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
for dbe in config["DbEntities"]:
    if dbe['ActiveInactive']=='true' and  dbe['Location']==DBEntity:
        CompanyName=dbe['Name']
        CompanyName=CompanyName.replace(" ","")
    try:
        
        logger = Logger()
        entityLocation = DBName+EntityName
        pah = spark.read.format("parquet").load(STAGE1_PATH+"/Purchase Header Archive").drop("YearMonth")
        DSE=spark.read.format("parquet").load(STAGE2_PATH+"/"+"Masters/DSE").drop("DBName","EntityName")
        ph=spark.read.format("parquet").load(STAGE1_PATH+"/Purchase Header")
        ph=ph.withColumn("PromisedReceiptDate",when((year(ph.PromisedReceiptDate)<1900)|(ph.PromisedReceiptDate.isNull()),to_date(ph.PostingDate)).otherwise(to_date(ph.PromisedReceiptDate)))
        ph=ph.select("No_","DimensionSetID","PromisedReceiptDate")
        ph=ph.withColumn("Flag",lit("Open"))
        w = Window.partitionBy('No_')
        pah=pah.withColumn('Version_No_max', f.max('VersionNo_').over(w))\
                                .where(f.col('VersionNo_') == f.col('Version_No_max')).drop("Version_No_max")
        pah=pah.withColumn('LinkVersionKey',concat_ws('|',pah.No_,pah.VersionNo_))
        pah = pah.filter((year(col("PostingDate"))!='1753'))
        pal = spark.read.format("parquet").load(STAGE1_PATH+"/Purchase Line Archive").drop("DBName","EntityName","PostingDate")
        w1 = Window.partitionBy('DocumentNo_')
        pal=pal.withColumn('Version_No_max', f.max('VersionNo_').over(w1))\
                                .where(f.col('VersionNo_') == f.col('Version_No_max')).drop("Version_No_max")
        pal=pal.withColumn('LinkVersionKey',concat_ws('|',pal.DocumentNo_,pal.VersionNo_))
        cond = [pal.LinkVersionKey == pah.LinkVersionKey]
        Purchase = Kockpit.LJOIN(pal,pah,cond)
        cond2 = [Purchase.DocumentNo_ == ph.No_]
        Purchase = Kockpit.LJOIN(Purchase,ph,cond2)
        Purchase= Purchase.withColumn("NOD",datediff(Purchase['PromisedReceiptDate'],lit(datetime.datetime.today())))
        PDDBucket =spark.read.format("parquet").load(STAGE1_Configurator_Path+"/tblPDDBucket")
        Maxoflt = PDDBucket.filter(PDDBucket['BucketName']=='<')
        MaxLimit = int(Maxoflt.select('MaxLimit').first()[0])
        Minofgt = PDDBucket.filter(PDDBucket['BucketName']=='>')
        MinLimit = int(Minofgt.select('MinLimit').first()[0])
        Purchase = Purchase.join(PDDBucket,PromisedReceiptDate == PDDBucket.Nod,'left').drop('ID','UpperLimit','LowerLimit')
        Purchase=Purchase.withColumn('BucketName',when(Purchase.PromisedReceiptDate>=MinLimit,lit(str(MinLimit)+'+')).otherwise(Purchase.BucketName))\
                    .withColumn('Nod',when(Purchase.PromisedReceiptDate>=MinLimit,Purchase.PromisedReceiptDate).otherwise(Purchase.Nod))
        Purchase=Purchase.withColumn('BucketName',when(Purchase.PromisedReceiptDate<=(MaxLimit),lit("Not Due")).otherwise(Purchase.BucketName))\
                    .withColumn('Nod',when(Purchase.PromisedReceiptDate<=(MaxLimit), Purchase.PromisedReceiptDate).otherwise(Purchase.Nod))
        Purchase = Purchase.join(DSE,"DimensionSetID",'left')
        Purchase = Kockpit.RenameDuplicateColumns(Purchase)
        Purchase.coalesce(1).write.format("parquet").mode("overwrite").option("overwriteSchema", "true").save(STAGE2_PATH+"/"+"Purchase/PurchaseArchive2")
              
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

