from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import csv, io,re,keyring,os,datetime
from datetime import timedelta, date
from pyspark.sql.functions import col,min as min_,max as max_,concat,concat_ws,year,when,month,to_date,lit,quarter,expr,sum,last_day,datediff,ltrim
import datetime,time,sys,calendar
from pyspark.sql.types import *
import re, logging
from builtins import str
import pandas as pd
import traceback
import os,sys,subprocess
from os.path import dirname, join, abspath
from distutils.command.check import check
import datetime as dt

helpersDir = abspath(join(join(dirname(__file__), '..'),'..'))


sys.path.insert(0, helpersDir)
from ConfigurationFiles.AppConfig import *
from Helpers.Constants import *
from Helpers.udf import *
from Helpers.DBInfo import *

def purchase_PayableWithoutSnapshots(sqlCtx, spark):
    st = dt.datetime.now()
    logger = Logger()

    try:
        VLE_Entity = next (table for table in config["TablesToIngest"] if table["Table"] == "Vendor Ledger Entry")
        DVLE_Entity = next (table for table in config["TablesToIngest"] if table["Table"] == "Detailed Vendor Ledg_ Entry")
        VPG_Entity = next (table for table in config["TablesToIngest"] if table["Table"] == "Vendor Posting Group")
        PIH_Entity = next (table for table in config["TablesToIngest"] if table["Table"] == "Purch_ Inv_ Header")
        for entityObj in config["DbEntities"]:
            logger = Logger()
            entityLocation = entityObj["Location"]
            DBName = entityLocation[:3]
            EntityName = entityLocation[-2:]
            hdfspath = STAGE1_PATH + "/" + entityLocation
            postgresUrl = PostgresDbInfo.url.format(entityLocation)
            
            vle = ToDFWitoutPrefix(sqlCtx, hdfspath, VLE_Entity,True)
            vle = vle.withColumnRenamed("DocumentNo_","VLE_Document_No")\
                .withColumnRenamed("EntryNo_", "VLE_No")
            pih = ToDFWitoutPrefix(sqlCtx, hdfspath, PIH_Entity,True)
            pih = pih.select('No_','PaymentTermsCode').withColumnRenamed('No_','PIH_No')
            dvle = ToDFWitoutPrefix(sqlCtx, hdfspath, DVLE_Entity,True)
            dvle = dvle.withColumnRenamed("VendorLedgerEntryNo_", "DVVLE_No")\
                        .withColumnRenamed("PostingDate", "DVLE_PostingDate")
            vpg = ToDFWitoutPrefix(sqlCtx, hdfspath, VPG_Entity,True)
            
            vle = vle.join(pih,vle["VLE_Document_No"]==pih["PIH_No"],'left')
            cond = [vle.VLE_No==dvle.DVVLE_No]
            df = RJOIN(vle, dvle, cond)
            df1 = RENAME(vpg, {'Code':'VendorPostingGroup','PayablesAccount':'GLAccount'})
            
            df2 = df.join(df1,'VendorPostingGroup','left')
            
            dse = sqlCtx.read.parquet(hdfspath + "/DSE").drop("DBName","EntityName")
            APsnapshots = LJOIN(df2,dse,'DimensionSetID')
            
            APsnapshots = RenameDuplicateColumns(APsnapshots)
            APsnapshots.cache()
            APsnapshots.write.jdbc(url=postgresUrl, table="Purchase.Payables", mode='overwrite', properties=PostgresDbInfo.props)
            
            logger.endExecution()
            
            try:
                    IDEorBatch = sys.argv[1]
            except Exception as e :
                    IDEorBatch = "IDE"
            log_dict = logger.getSuccessLoggedRecord("Purchase.Payables", DBName, EntityName, APsnapshots.count(), len(APsnapshots.columns), IDEorBatch)
            log_df = spark.createDataFrame(log_dict, logger.getSchema())
            log_df.write.jdbc(url=PostgresDbInfo.logsDbUrl, table="logtable", mode='append', properties=PostgresDbInfo.props)

    except Exception as ex:
        exc_type,exc_value,exc_traceback=sys.exc_info()
        logging.error("Error: ",sys.exc_info)
        print("Error:",ex)
        print("type - "+str(exc_type))
        print("File - "+exc_traceback.tb_frame.f_code.co_filename)
        print("Error Line No. - "+str(exc_traceback.tb_lineno))
        
        logger.endExecution()
        
        try:
                IDEorBatch = sys.argv[1]
        except Exception as e :
                IDEorBatch = "IDE"
        
        log_dict = logger.getErrorLoggedRecord('Purchase.Payables', DBName, EntityName, str(ex), str(exc_traceback.tb_lineno), IDEorBatch)
        log_df = spark.createDataFrame(log_dict, logger.getSchema())
        log_df.write.jdbc(url=PostgresDbInfo.logsDbUrl, table="logtable", mode='append', properties=PostgresDbInfo.props)
    print('purchase_PayableWithoutSnapshots completed: ' + str((dt.datetime.now()-st).total_seconds()))
    
if __name__ == "__main__":
    sqlCtx, spark = getSparkConfig(SPARK_MASTER, "Stage2:PayableWithoutSnapshots")
    purchase_PayableWithoutSnapshots(sqlCtx, spark)