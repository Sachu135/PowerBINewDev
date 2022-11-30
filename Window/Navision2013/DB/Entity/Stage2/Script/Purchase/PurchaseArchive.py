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
sqlCtx, spark = getSparkConfig("local[*]", "Stage2:PurchaseArchive")
def purchase_PurchaseArchive():
    
    for dbe in config["DbEntities"]:
        if dbe['ActiveInactive']=='true' and  dbe['Location']==DBEntity:
            CompanyName=dbe['Name']
            CompanyName=CompanyName.replace(" ","")
        try:
                
            logger = Logger()
            entityLocation = DBName+EntityName 
            pah = spark.read.format("parquet").load(STAGE1_PATH+"/Purchase Header Archive")
            ph=spark.read.format("parquet").load(STAGE1_PATH+"/Purchase Header")
            pal = spark.read.format("parquet").load(STAGE1_PATH+"/Purchase Line Archive").drop("DBName","EntityName","PostingDate")
            DSE=spark.read.format("parquet").load(STAGE2_PATH+"/"+"Masters/DSE").drop("DBName","EntityName")
            ph=ph.select("No_","DimensionSetID")
            ph=ph.withColumn("Flag",lit("Open"))
            w = Window.partitionBy('No_')
            pah=pah.withColumn('Version_No_max', f.max('VersionNo_').over(w))\
                                    .where(f.col('VersionNo_') == f.col('Version_No_max')).drop("Version_No_max")
            pah=pah.withColumn('LinkVersionKey',concat_ws('|',pah.No_,pah.VersionNo_))
            pah = pah.filter((year(col("PostingDate"))!='1753'))
            
            w1 = Window.partitionBy('DocumentNo_')
            pal=pal.withColumn('Version_No_max', f.max('VersionNo_').over(w1))\
                                    .where(f.col('VersionNo_') == f.col('Version_No_max')).drop("Version_No_max")
            pal=pal.withColumn('LinkVersionKey',concat_ws('|',pal.DocumentNo_,pal.VersionNo_))
            cond = [pal.LinkVersionKey == pah.LinkVersionKey]
            Purchase = Kockpit.LJOIN(pal,pah,cond)
            cond2 = [Purchase.DocumentNo_ == ph.No_]
            Purchase = Kockpit.LJOIN(Purchase,ph,cond2)
            Purchase = Purchase.join(DSE,"DimensionSetID",'left')
            Purchase = Kockpit.RenameDuplicateColumns(Purchase)
            Purchase.coalesce(1).write.format("parquet").mode("overwrite").option("overwriteSchema", "true").save(STAGE2_PATH+"/"+"Purchase/PurchaseArchive")   
            logger.endExecution()
             
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"
            log_dict = logger.getSuccessLoggedRecord("Purchase.PurchaseArchive", DBName, EntityName, Purchase.count(), len(Purchase.columns), IDEorBatch)
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
            os.system("spark-submit "+Kockpit_Path+"/Email.py 1 PurchaseArchive '"+CompanyName+"' "+DBEntity+" "+str(exc_traceback.tb_lineno)+" ")
                    
            log_dict = logger.getErrorLoggedRecord('Purchase.PurchaseArchive', DBName, EntityName, str(ex), str(exc_traceback.tb_lineno), IDEorBatch)
            log_df = spark.createDataFrame(log_dict, logger.getSchema())
            log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)
    print('purchase_PurchaseArchive completed: ' + str((dt.datetime.now()-st).total_seconds()))
  
if __name__ == '__main__':
    
    purchase_PurchaseArchive()
