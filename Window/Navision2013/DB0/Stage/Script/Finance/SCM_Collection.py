from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession,Row
#from pyspark.sql.functions import *
from pyspark.sql import functions as f
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import * #regexp_replace, col, udf, broadcast, lit
import datetime, time
import datetime as dt
from datetime import datetime
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


def finance_scm_collection(sqlCtx, spark):
    st = dt.datetime.now()
    logger = Logger()

    try:
        todayDate = datetime.datetime.today().strftime('%Y-%m-%d')

        cdEntity = next (table for table in config["TablesToIngest"] if table["Table"] == "Collection Details")
        mptEntity = next (table for table in config["TablesToIngest"] if table["Table"] == "Multiple Payment Terms")
        cleEntity = next (table for table in config["TablesToIngest"] if table["Table"] == "Cust_ Ledger Entry")
        dpEntity = next (table for table in config["TablesToIngest"] if table["Table"] == "Delivery Pending")
        i2Entity = next (table for table in config["TablesToIngest"] if table["Table"] == "Installation2")
        
        
        for entityObj in config["DbEntities"]:
            logger = Logger()
            entityLocation = entityObj["Location"]
            DBName = entityLocation[:3]
            EntityName = entityLocation[-2:]
            hdfspath = STAGE1_PATH + "/" + entityLocation
            postgresUrl = PostgresDbInfo.url.format(entityLocation)

            cdDF = ToDFWitoutPrefix(sqlCtx, hdfspath, cdEntity, False)\
                    .withColumnRenamed("RemainingAmount","CollectionRemainingAmount")\
                    .withColumnRenamed("ExpectedCollectionDate","ExpectedCollectionDate1")
                    
            mptDF = ToDFWitoutPrefix(sqlCtx, hdfspath, mptEntity, False)\
                    .withColumnRenamed("RemainingAmount","MPTRemainingAmount")
                    
            cleDF = ToDFWitoutPrefix(sqlCtx, hdfspath, cleEntity, False)
            dseDF = sqlCtx.read.parquet(hdfspath+"/DSE").drop("DBName","EntityName")
            dpDF = ToDFWitoutPrefix(sqlCtx, hdfspath, dpEntity, False)\
                    .where(~col('DeliveryStatus').isin([8,9]))\
                    .withColumnRenamed('ActualDeliveryDate', 'ActualDeliveryDateTemp')
            
            i2DF = ToDFWitoutPrefix(sqlCtx, hdfspath, i2Entity, False).where(~col('CurrentStatus').isin([7]))
            
            resultDF = cdDF.join(mptDF, [cdDF.InvoiceNo_ == mptDF.DocumentNo], 'left')\
                        .join(cleDF, [cleDF.DocumentNo_ == cdDF.InvoiceNo_], 'left')\
                        .join(dseDF, [cleDF.DimensionSetID == dseDF.DimensionSetID], 'left')\
                        .join(dpDF, [dpDF.InvoiceNo_ == cdDF.InvoiceNo_], 'left')\
                        .join(i2DF, [i2DF.InvoiceNo_ == cdDF.InvoiceNo_], 'left')\
                        .withColumn("DimSetID", when(cleDF.DimensionSetID == '', lit(0)).otherwise(lit(1)))\
                        .withColumn("DueDate", when(mptDF.ActualDueDate.isNull(), cdDF.DocumentationDueDate).otherwise(mptDF.ActualDueDate))\
                        .withColumn("ActualRemainingAmount", when(mptDF.MPTRemainingAmount.isNull(), cdDF.CollectionRemainingAmount).otherwise(mptDF.MPTRemainingAmount))\
                        .withColumn("SalePaymentTermType", when(mptDF.SalePaymentTermType.isNull(), lit(0)).otherwise(mptDF.SalePaymentTermType))\
                        .withColumn("DeliveryDateFlag", when(dpDF.ActualDeliveryDateTemp.isNull(), lit(0)).otherwise(lit(1)))\
                        .withColumn("InstallationFinishDate", when(i2DF.InstallationFinishDate.isNull(), when(dpDF.ActualDeliveryDateTemp.isNull(), when(mptDF.ActualDueDate.isNull(), cdDF.DocumentationDueDate).otherwise(mptDF.ActualDueDate)).otherwise(dpDF.ActualDeliveryDateTemp)).otherwise(i2DF.InstallationFinishDate))\
                        .withColumn("ActualDeliveryDate", when(dpDF.ActualDeliveryDateTemp.isNull(), cdDF.DocumentationDueDate).otherwise(dpDF.ActualDeliveryDateTemp))\
                        .withColumn("Today", to_date(lit(todayDate)))\
                        .select(mptDF.PaymentStatus, mptDF.Percentage, cdDF.InvoiceNo_, cdDF.InvoiceDate, cdDF.Ext_Doc_No_, cdDF.CustomerNo_, cdDF.CollectionRemainingAmount, cdDF.SalesPersonCode, cdDF.PaymentTermsCode, cdDF.DocSubmissionDate, cdDF.DocumentationDueDate, cdDF.RemarksByCollectionTeam, mptDF.ActualDueDate, cdDF.RSM, cdDF.RegionName, cdDF.BranchName, cdDF.Remarks, cdDF.ExpectedCollectionDate1, cdDF.ExpectedCollectionDate2, cdDF.ExpectedCollectionDate3, cdDF.ExpectedCollectionDate4, cdDF.PaymentStage, cdDF.PaymentDelayReason, cdDF.FinancePerson\
                            ,"Today", "DimSetID", "DueDate", mptDF.MPTRemainingAmount, "ActualRemainingAmount", "SalePaymentTermType", "ActualDeliveryDate", "DeliveryDateFlag", "InstallationFinishDate"\
                            ,dseDF.Link_CUSTOMER, dseDF.Link_CUSTOMERKey, dseDF.Link_BRANCH, dseDF.Link_BRANCHKey, dseDF.Link_TARGETPROD, dseDF.Link_TARGETPRODKey, dseDF.Link_OTBRANCH, dseDF.Link_OTBRANCHKey, dseDF.Link_SUBBU, dseDF.Link_SUBBUKey, dseDF.Link_SBU, dseDF.Link_SBUKey, dseDF.Link_PRODUCT, dseDF.Link_PRODUCTKey)
            
            resultDF = resultDF.selectExpr("*"
                        ,"CASE WHEN ActualRemainingAmount = 0 THEN 'Collected' ELSE CASE WHEN DueDate = '1753-01-01' THEN 'Task Pending' ELSE 'Collectable' END END AS Collection_Flag"\
                        ,"CASE WHEN DueDate = '1753-01-01' THEN - 1 ELSE DATEDIFF(Today, DueDate) END AS NOD"\
                        ,"CASE WHEN InvoiceDate = '1753-01-01' THEN - 1 ELSE DATEDIFF(Today, InvoiceDate) END AS NOD_Invoice"\
                        ,"CASE WHEN ActualDeliveryDate = '1753-01-01' THEN - 1 ELSE DATEDIFF(Today, ActualDeliveryDate) END AS NOD_Delivery"\
                        ,"CASE WHEN InstallationFinishDate = '1753-01-01' THEN - 1 ELSE DATEDIFF(Today, InstallationFinishDate) END AS NOD_Installation")


            finalDF = resultDF.selectExpr("*",\
                        "CASE WHEN ExpectedCollectionDate1 > ExpectedCollectionDate2 AND ExpectedCollectionDate1 > ExpectedCollectionDate3   AND ExpectedCollectionDate1 > ExpectedCollectionDate4   THEN ExpectedCollectionDate1  WHEN ExpectedCollectionDate2 > ExpectedCollectionDate1   AND ExpectedCollectionDate2 > ExpectedCollectionDate3   AND ExpectedCollectionDate2 > ExpectedCollectionDate4   THEN ExpectedCollectionDate2  WHEN ExpectedCollectionDate3 > ExpectedCollectionDate1   AND ExpectedCollectionDate3 > ExpectedCollectionDate2   AND ExpectedCollectionDate3 > ExpectedCollectionDate4   THEN ExpectedCollectionDate3  WHEN ExpectedCollectionDate4 > ExpectedCollectionDate2   AND ExpectedCollectionDate4 > ExpectedCollectionDate3   AND ExpectedCollectionDate4 > ExpectedCollectionDate1   THEN ExpectedCollectionDate4  ELSE '1753-01-01'  END AS ExpectedCollectionDate",\
                        "CASE WHEN NOD >= 0   AND NOD <= 30   THEN '0-30'  WHEN NOD >= 31   AND NOD <= 60   THEN '31-60'  WHEN NOD >= 61   AND NOD <= 90   THEN '61-90'  WHEN NOD >= 91   AND NOD <= 120   THEN '91-120'  WHEN NOD >= 121   AND NOD <= 180   THEN '121-180'  WHEN NOD >= 181   AND NOD <= 270   THEN '181-270'  WHEN NOD >= 271   THEN '270+'  ELSE 'DUE'  END AS Interval",\
                        "CASE WHEN NOD_Invoice >= 0   AND NOD_Invoice <= 30   THEN '0-30'  WHEN NOD_Invoice >= 31   AND NOD_Invoice <= 60   THEN '31-60'  WHEN NOD_Invoice >= 61   AND NOD_Invoice <= 90   THEN '61-90'  WHEN NOD_Invoice >= 91   AND NOD_Invoice <= 120   THEN '91-120'  WHEN NOD_Invoice >= 121   AND NOD_Invoice <= 180   THEN '121-180'  WHEN NOD_Invoice >= 181   AND NOD_Invoice <= 270   THEN '181-270'  WHEN NOD_Invoice >= 271   THEN '270+'  ELSE 'DUE'  END AS Interval_Invoice",\
                        "CASE WHEN NOD_Delivery >= 0   AND NOD_Delivery <= 30   THEN '0-30'  WHEN NOD_Delivery >= 31   AND NOD_Delivery <= 60   THEN '31-60'  WHEN NOD_Delivery >= 61   AND NOD_Delivery <= 90   THEN '61-90'  WHEN NOD_Delivery >= 91   AND NOD_Delivery <= 120   THEN '91-120'  WHEN NOD_Delivery >= 121   AND NOD_Delivery <= 180   THEN '121-180'  WHEN NOD_Delivery >= 181   AND NOD_Delivery <= 270   THEN '181-270'  WHEN NOD_Delivery >= 271   THEN '270+'  ELSE 'DUE'  END AS Interval_Delivery",\
                        "CASE WHEN NOD_Installation >= 0   AND NOD_Installation <= 30   THEN '0-30'  WHEN NOD_Installation >= 31   AND NOD_Installation <= 60   THEN '31-60'  WHEN NOD_Installation >= 61   AND NOD_Installation <= 90   THEN '61-90'  WHEN NOD_Installation >= 91   AND NOD_Installation <= 120   THEN '91-120'  WHEN NOD_Installation >= 121   AND NOD_Installation <= 180   THEN '121-180'  WHEN NOD_Installation >= 181   AND NOD_Installation <= 270   THEN '181-270'  WHEN NOD_Installation >= 271   THEN '270+'  ELSE 'DUE'  END AS Interval_Installation")
            
            finalDF.cache()
            finalDF.write.jdbc(url=postgresUrl, table="finance.scm_collection", mode='overwrite', properties=PostgresDbInfo.props)#PostgresDbInfo.props

            logger.endExecution()
            
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"

            log_dict = logger.getSuccessLoggedRecord("Finance.scm_collection", DBName, EntityName, finalDF.count(), len(finalDF.columns), IDEorBatch)
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
        
        log_dict = logger.getErrorLoggedRecord('Finance.scm_collection', DBName, EntityName, str(ex), str(exc_traceback.tb_lineno), IDEorBatch)
        log_df = spark.createDataFrame(log_dict, logger.getSchema())
        log_df.write.jdbc(url=PostgresDbInfo.logsDbUrl, table="logtable", mode='append', properties=PostgresDbInfo.props)
    print('finance_scm_collection completed: ' + str((dt.datetime.now()-st).total_seconds()))

if __name__ == "__main__":
    sqlCtx, spark = getSparkConfig4g(SPARK_MASTER, "Stage2:SCM_Collection")
    finance_scm_collection(sqlCtx, spark)
