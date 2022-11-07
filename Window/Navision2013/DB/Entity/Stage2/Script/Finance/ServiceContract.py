'''
Created on 05 Mar 2021

@author: Prashant
'''
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import csv, io,re,keyring,os,datetime
from datetime import timedelta, date
from pyspark.sql.functions import col,max as max_,concat,concat_ws,year,when,month,to_date,lit,quarter,expr,sum,last_day,datediff,length,trim
import datetime,time,sys,calendar
from pyspark.sql.types import *
import re
from builtins import str
import pandas as pd
import traceback
import os,sys,subprocess
from os.path import dirname, join, abspath
from distutils.command.check import check
import datetime, time
import datetime as dt
from datetime import datetime

helpersDir = '/home/padmin/KockpitStudio'
sys.path.insert(0, helpersDir)
from ConfigurationFiles.AppConfig import *
from Helpers.Constants import *
from Helpers.udf import *

Datelog = datetime.datetime.now().strftime('%Y-%m-%d')
start_time = datetime.datetime.now()
start_time_string = start_time.strftime('%H:%M:%S')

def finance_ServiceContract(sqlCtx, spark):
    st = dt.datetime.now()
    logger = Logger()
    Configurl = "jdbc:postgresql://192.10.15.134/Configurator_Linux"

    try:
        schEntity = next (table for table in config["TablesToIngest"] if table["Table"] == "Service Contract Header")
        sclEntity = next (table for table in config["TablesToIngest"] if table["Table"] == "Service Contract Line")
        
        for entityObj in config["DbEntities"]:
            logger = Logger()
            entityLocation = entityObj["Location"]
            DBName = entityLocation[:3]
            EntityName = entityLocation[-2:]
            hdfspath = STAGE1_PATH + "/" + entityLocation
            postgresUrl = PostgresDbInfo.url.format(entityLocation)
            Datelog = datetime.datetime.now().strftime('%Y-%m-%d')
            CurrentYear = Datelog.split('-')[0]
            CurrentMonth = Datelog.split('-')[1]
            YM = int(CurrentYear+CurrentMonth)
            ####################### DB Credentials  ###########################
            #csvpath=os.path.dirname(os.path.realpath(__file__))
            #AccDt = sqlctx.read.parquet(hdfspath+"/Data/AccessDetails").filter(col('NewDBName')==DB).collect()
            Query_Company="(SELECT *\
                            FROM "+chr(34)+"tblCompanyName"+chr(34)+") AS df"
            Company = spark.read.format("jdbc").options(url=PostgresDbInfo.Configurl, dbtable=Query_Company,\
                            user=PostgresDbInfo.props["user"],password=PostgresDbInfo.props["password"],driver= PostgresDbInfo.props["driver"]).load()
            Company = Company.filter(col('DBName')==DBName).filter(col('NewCompanyName') == EntityName)
            Company = Company.select("StartDate","EndDate")
            Calendar_StartDate = Company.select('StartDate').rdd.flatMap(lambda x: x).collect()[0]
            Calendar_StartDate = datetime.datetime.strptime(Calendar_StartDate,"%m/%d/%Y").date()
            
            if datetime.date.today().month>int(MnSt)-1:
                    UIStartYr=datetime.date.today().year-int(yr)+1
            else:
                    UIStartYr=datetime.date.today().year-int(yr)
            UIStartDate=datetime.date(UIStartYr,int(MnSt),1)
            UIStartDate=max(Calendar_StartDate,UIStartDate)
            ####################### Upper Bound  ###########################
            Query_FieldSelection="(SELECT *\
                                FROM "+chr(34)+"tblFieldSelection"+chr(34)+") AS df"
            table = spark.read.format("jdbc").options(url=PostgresDbInfo.Configurl, dbtable=Query_FieldSelection,\
                                    user=PostgresDbInfo.props["user"],password=PostgresDbInfo.props["password"],driver= PostgresDbInfo.props["driver"]).load()
            table = table.filter(table['Flag'] == 1 ).filter(table['TableName'].isin(['Order Date','Sales Order']))
            FieldName = table.select("FieldType","Flag","TableName")
            FieldName1=FieldName.filter("TableName=='Order Date'").select('FieldType').collect()[0]["FieldType"]
            FieldName2=FieldName.filter("TableName=='Sales Order'").select('FieldType').collect()[0]["FieldType"]
            FieldName1 = re.sub("[\s+]",'',FieldName1)
            FieldName2 = re.sub("[\s+]",'',FieldName2)
            sch = ToDFWitoutPrefix(sqlCtx, hdfspath, schEntity,False)
            sch = sch.select('ContractNo_','ContractType','Status','DimensionSetID','Description','CustomerNo_','StartingDate','ContractExpired','ExpirationDate','AnnualAmount')\
                    .withColumnRenamed('ContractType','HeaderContractType')\
                    .withColumnRenamed('Description','HeaderDescription')\
                    .withColumnRenamed('Status','HeaderStatus')\
                    .withColumnRenamed('StartingDate','LinkDate')
            scl = ToDFWitoutPrefix(sqlCtx, hdfspath, sclEntity,False)
            scl = scl.select('ContractNo_','ContractType','ContractStatus','ServiceItemNo_','Description',
                                'SerialNo_','ItemNo_','LineAmount')\
                    .withColumnRenamed('ContractType','LineContractType')\
                    .withColumnRenamed('Description','LineDescription')\
                    .withColumnRenamed('ContractStatus','LineStatus')\
                    .withColumnRenamed('LineAmount','Amount')\
                    .withColumnRenamed('ServiceItemNo_','LinkItem')

            ServiceContract = scl.join(sch,'ContractNo_','left')
            ServiceContract = ServiceContract.withColumnRenamed('CustomerNo_','LinkCustomer')
            ServiceContract = ServiceContract.withColumn('LinkDate',to_date(ServiceContract['LinkDate']))
            ServiceContract = ServiceContract.filter(ServiceContract['LinkDate']>=UIStartDate)
            
            ServiceContract = ServiceContract.withColumn('DBName',lit(DBName))\
                                            .withColumn('EntityName',lit(EntityName))
            ############################## Sales Person Field Selection #################################
            DSE = sqlCtx.read.parquet(hdfspath + "/DSE").drop("DBName","EntityName")
            ServiceContract =ServiceContract.join(DSE,"DimensionSetID",'left')
            ServiceContract = ServiceContract.na.fill({'LinkCustomer':'NA','LinkDate':'NA','LinkItem':'NA'})
            finalDF = ServiceContract.withColumn("LinkCustomerKey",concat_ws("|",ServiceContract.DBName,ServiceContract.EntityName,ServiceContract.LinkCustomer))\
                    .withColumn("LinkDateKey",concat_ws("|",ServiceContract.DBName,ServiceContract.EntityName,ServiceContract.LinkDate))\
                    .withColumn("LinkItemKey",concat_ws("|",ServiceContract.DBName,ServiceContract.EntityName,ServiceContract.LinkItem))

            finalDF.cache()
            finalDF.write.jdbc(url=postgresUrl, table="Finance.ServiceContract", mode='overwrite', properties=PostgresDbInfo.props)
            logger.endExecution()
                
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"

            log_dict = logger.getSuccessLoggedRecord("Finance.ServiceContract", DBName, EntityName, finalDF.count(), len(finalDF.columns), IDEorBatch)
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
        
        log_dict = logger.getErrorLoggedRecord('Finance.ServiceContract', DBName, EntityName, str(ex), str(exc_traceback.tb_lineno), IDEorBatch)
        log_df = spark.createDataFrame(log_dict, logger.getSchema())
        log_df.write.jdbc(url=PostgresDbInfo.logsDbUrl, table="logtable", mode='append', properties=PostgresDbInfo.props)
    print('finance_ServiceContract completed: ' + str((dt.datetime.now()-st).total_seconds()))
    

if __name__ == "__main__":
    sqlCtx, spark = getSparkConfig4g(SPARK_MASTER, "Stage2:ServiceContract")
    finance_ServiceContract(sqlCtx, spark)