from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession,Row
#from pyspark.sql.functions import *
from pyspark.sql import functions as f
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import regexp_replace, col, udf, broadcast
import datetime, time
import datetime as dt
import re

from pyspark.sql import functions as F
import pandas as pd
import os,sys,subprocess
from os.path import dirname, join, abspath

helpersDir = '/home/padmin/KockpitStudio'
sys.path.insert(0, helpersDir)
from ConfigurationFiles.AppConfig import *
from Helpers.Constants import *
from Helpers.udf import *
from Helpers.DBInfo import *

def purchase_PurchaseReceipt(sqlCtx, spark):
    st = dt.datetime.now()
    logger = Logger()

    try:
        
        phEntity = next (table for table in config["TablesToIngest"] if table["Table"] == "Purch_ Rcpt_ Header")
        plEntity = next (table for table in config["TablesToIngest"] if table["Table"] == "Purch_ Rcpt_ Line")

        for entityObj in config["DbEntities"]:
            logger = Logger()
            entityLocation = entityObj["Location"]
            DBName = entityLocation[:3]
            EntityName = entityLocation[-2:]
            hdfspath = STAGE1_PATH + "/" + entityLocation
            postgresUrl = PostgresDbInfo.url.format(entityLocation)

            phDF = ToDFWitoutPrefix(sqlCtx, hdfspath, phEntity,True)
            plDF = ToDFWitoutPrefix(sqlCtx, hdfspath, plEntity,True)
            plDF = plDF.withColumnRenamed('PostingDate','Line_PostingDate')

            Query_Company="(SELECT * FROM "+chr(34)+"tblCompanyName"+chr(34)+") AS df"
            Company = spark.read.format("jdbc").options(url=PostgresDbInfo.Configurl, dbtable=Query_Company,\
                            user=PostgresDbInfo.props["user"], password=PostgresDbInfo.props["password"], driver= PostgresDbInfo.props["driver"]).load()
            Company = Company.filter(col('DBName')==DBName).filter(col('NewCompanyName') == EntityName)
            Calendar_StartDate = Company.select('StartDate').rdd.flatMap(lambda x: x).collect()[0]
            Calendar_StartDate = datetime.datetime.strptime(Calendar_StartDate,"%m/%d/%Y").date()
            
            if datetime.date.today().month>int(MnSt)-1:
                UIStartYr=datetime.date.today().year-int(yr)+1
            else:
                UIStartYr=datetime.date.today().year-int(yr)
            
            UIStartDate=datetime.date(UIStartYr,int(MnSt),1)
            UIStartDate=max(Calendar_StartDate,UIStartDate)

            finalDF = plDF.join(phDF, phDF['No_']==plDF['DocumentNo_'], 'left')
            finalDF = finalDF.filter(finalDF['PostingDate']>=UIStartDate)
            
            finalDF = RenameDuplicateColumns(finalDF)
            finalDF.cache()
            finalDF.write.jdbc(url=postgresUrl, table="Purchase.PurchaseReceipt", mode='overwrite', properties=PostgresDbInfo.props)
            
            logger.endExecution()

            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"

            log_dict = logger.getSuccessLoggedRecord("Purchase.PurchaseReceipt", DBName, EntityName, finalDF.count(), len(finalDF.columns), IDEorBatch)
            log_df = spark.createDataFrame(log_dict, logger.getSchema())
            log_df.write.jdbc(url=PostgresDbInfo.logsDbUrl, table="logtable", mode='append', properties=PostgresDbInfo.props)

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

        log_dict = logger.getErrorLoggedRecord('Purchase.PurchaseReceipt', DBName, EntityName, str(ex), str(exc_traceback.tb_lineno), IDEorBatch)
        log_df = spark.createDataFrame(log_dict, logger.getSchema())
        log_df.write.jdbc(url=PostgresDbInfo.logsDbUrl, table="logtable", mode='append', properties=PostgresDbInfo.props)
    print('purchase_PurchaseReceipt completed: ' + str((dt.datetime.now()-st).total_seconds()))
    
if __name__ == "__main__":
    sqlCtx, spark = getSparkConfig(SPARK_MASTER, "Stage2:PurchaseReceipt")
    purchase_PurchaseReceipt(sqlCtx, spark)