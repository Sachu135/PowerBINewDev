'''
Created on 9 Feb 2019
@author: Lalit
'''

from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import csv, io,re,keyring,os,datetime
from datetime import timedelta, date
from pyspark.sql.functions import col,max as max_,concat,concat_ws,year,when,month,to_date,lit,quarter,expr,sum,datediff,length,trim
import datetime,time,sys,calendar
from pyspark.sql.types import *
import re
from builtins import str
import pandas as pd
import traceback
import datetime as dt

helpersDir = '/home/padmin/KockpitStudio'
sys.path.insert(0, helpersDir)
from ConfigurationFiles.AppConfig import *
from Helpers.Constants import *
from Helpers.udf import *
from Helpers import udf as Kockpit
start_time = dt.datetime.now()
schema_log = StructType([
                        StructField('Date',StringType(),True),
                        StructField('Start_Time',StringType(),True),
                        StructField('End_Time', StringType(),True),
                        StructField('Run_Time',StringType(),True),
                        StructField('File_Name',StringType(),True),
                        StructField('DB',StringType(),True),
                        StructField('EN', StringType(),True),
                        StructField('Status',StringType(),True),
                        StructField('Log_Status',StringType(),True),
                        StructField('ErrorLineNo.',StringType(),True),
                        StructField('Rows',IntegerType(),True),
                        StructField('Columns',IntegerType(),True),
                        StructField('Source',StringType(),True)
                ])
print("Execution Start - IAP")
import psycopg2

def inventory_IAP(sqlCtx, spark):
    #locals1 = locals()
    st = dt.datetime.now()
    logger = Logger()
    try:
        def Removelineage(df,x):
            exec(""+df+"%d"%x+".cache()")
            return eval("%s%d"%(df,x))
        
        IAE_Entity = next (table for table in config["TablesToIngest"] if table["Table"] == "Item Application Entry")
        
        for entityObj in config["DbEntities"]:
            logger = Logger()
            entityLocation = entityObj["Location"]
            DBName = entityLocation[:3]
            EntityName = entityLocation[-2:]
            hdfspath = STAGE1_PATH + "/" + entityLocation
            postgresUrl = PostgresDbInfo.url.format(entityLocation)
            count = 1
            flag=2
            try:
                Query_IAPCheck = "(SELECT * FROM inventory.iap_readonly) AS COA"
                IAPCheck = spark.read.format("jdbc").options(url=postgresUrl, dbtable=Query_IAPCheck,\
                                                    user=PostgresDbInfo.props["user"],password=PostgresDbInfo.props["password"],driver= PostgresDbInfo.props["driver"]).load()
                
                #IAPCheck = IAPCheck.withColumnRenamed('EntryNo_','Entry No_')
                flag=1
            except:
                flag=0
            print(flag,"<---------------This is flag")
            #sys.exit()
            IAE = ToDFWitoutPrefix(sqlCtx, hdfspath, IAE_Entity,True)
            IAE.registerTempTable("ItemApplicationEntry")
            global ItemApp1,NotDoneLot1,Ref,NotDoneLot,Ref1
            if flag==1:
                #iappath = hdfspath + "/Stage2/" + DBET + "/iap1"
                ####################### DB Credentials  ###########################
                #iae = sqlctx.read.parquet(hdfspath+"/Stage1/"+DBET+"/ItemApplicationEntry")
                #iae = IAE.withColumnRenamed('EntryNo_','Entry No_')
                #iae = iae.drop('DBName')
                iae = IAE.\
                        withColumn('ILEInbound',concat(col('ItemLedgerEntryNo_'),lit('|'),col('InboundItemEntryNo_'))).\
                        withColumn('OutboundTrf',concat(col('OutboundItemEntryNo_'),lit('|'),col('Transferred-fromEntryNo_')))
                #iap = sqlctx.read.format("jdbc").options(url=Postgresurl,dbtable="db1e1.iap_inc",user=PGUSER,password=PGPWD,driver= "org.postgresql.Driver").load()
                # IAPCheck = IAPCheck.filter(IAPCheck['RefNo']==0)
                # print(IAPCheck.count())
                # exit()
                iap = IAPCheck.select(col('EntryNo_'),\
                                    col('RefNo'),\
                                    col('LotNo')).filter(col('RefNo')!=0).filter(col('RefFlag')=='Mapped')
                iaejoiniap = iae.join(iap,on=['EntryNo_'],how='left')
                iaejoiniap = iaejoiniap.na.fill(0)
                iaejoiniap.persist()
                mapped = iaejoiniap.filter(iaejoiniap['RefNo']!=0)
                unmapped = iaejoiniap.filter(iaejoiniap['RefNo']==0)
                # print(mapped.count(),unmapped.count())
                # exit()
                done = unmapped.filter(unmapped['OutboundItemEntryNo_']==0).\
                    withColumn('RefNo',col('ItemLedgerEntryNo_')).\
                    withColumn('LotNo',lit(1))
                   
                notdone = unmapped.filter(unmapped['OutboundItemEntryNo_']!=0)
                # print(notdone.count())
                # exit()
                #putting new done values in mappeddf
                mapped = mapped.union(done)
                # print(mapped.count(),unmapped.count())
                # exit()
                ItemApp1 = mapped.select(col('EntryNo_'),\
                                    col('ItemLedgerEntryNo_'),\
                                    col('InboundItemEntryNo_').alias('Inbound'), \
                                    col('OutboundItemEntryNo_').alias('Outbound'), \
                                    col('Quantity').alias('Quantity'), \
                                    col('Transferred-fromEntryNo_').alias('TrfFrom'), \
                                    col('ILEInbound'),\
                                    col('OutboundTrf'),\
                                    col('PostingDate'), \
                                    col('RefNo'), \
                                    col('LotNo')).\
                                    withColumn('RefFlag',lit('Mapped'))
                
                NotDoneLot1 = notdone.select(col('EntryNo_'), \
                                        col('ItemLedgerEntryNo_'), \
                                        col('InboundItemEntryNo_').alias('Inbound'), \
                                        col('OutboundItemEntryNo_').alias('Outbound'), \
                                        col('Quantity'), \
                                        col('Transferred-fromEntryNo_').alias('TrfFrom'),\
                                        col('ILEInbound'),\
                                        col('OutboundTrf'),\
                                        col('PostingDate')).\
                                        withColumn('Link_In',when(col('TrfFrom')!=0,col('OutboundTrf')).otherwise('NA')).\
                                        withColumn('Link_Out',when(col('TrfFrom')==0,col('Inbound')).otherwise(-1))
                # print(NotDoneLot1.count())
                # exit()
                #test="(SELECT * FROM "+DBET+".tempnotdonelot ) AS itemapp"
                ItemApp1.persist()
                ItemApp1.registerTempTable("ItemApp1")
                #NotDoneLot1.registerTempTable("NotDoneLot1")
                ItemApp1.write.jdbc(url=postgresUrl, table="Inventory.iap", mode=owmode, properties=PostgresDbInfo.props)#R
                #exit()
                NotDoneLot1.persist()
                NotDoneLot1.registerTempTable("NotDoneLot1")
                #NotDoneLot1.toPandas().to_csv('notdonelot.csv')
                # print(NotDoneLot1.count())
                # exit()
            if flag == 0:
                ItemApp1=spark.sql("SELECT EntryNo_,\
                                     ItemLedgerEntryNo_,\
                                     InboundItemEntryNo_ as Inbound,\
                                     OutboundItemEntryNo_ as Outbound,\
                                     Quantity,\
                                     `Transferred-fromEntryNo_` as TrfFrom,\
                                     concat(ItemLedgerEntryNo_,'|',InboundItemEntryNo_) AS ILEInbound,\
                                     concat(OutboundItemEntryNo_,'|',`Transferred-fromEntryNo_`) AS OutboundTrf,\
                                     PostingDate,\
                                     ItemLedgerEntryNo_ AS RefNo,\
                                     1 AS LotNo,\
                                     'Mapped' AS RefFlag \
                                   FROM ItemApplicationEntry WHERE OutboundItemEntryNo_=0")
                
                NotDoneLot1= spark.sql("SELECT EntryNo_,\
                                     ItemLedgerEntryNo_,\
                                     InboundItemEntryNo_ as Inbound,\
                                     OutboundItemEntryNo_ as Outbound,\
                                     Quantity, \
                                     Case When `Transferred-fromEntryNo_` is null then 0 else `Transferred-fromEntryNo_` end as TrfFrom,\
                                     Case When Quantity<0 Then 'NA' Else\
                                       Case When `Transferred-fromEntryNo_`<>0 Then \
                                       concat(OutboundItemEntryNo_,'|',`Transferred-fromEntryNo_`) \
                                       Else OutboundItemEntryNo_ End End AS Link_In,\
                                     Case When Quantity<0 Then InboundItemEntryNo_ Else -1 End AS Link_Out,\
                                     concat(ItemLedgerEntryNo_,'|',InboundItemEntryNo_) AS ILEInbound,\
                                     concat(OutboundItemEntryNo_,'|',`Transferred-fromEntryNo_`) AS OutboundTrf,\
                                     PostingDate \
                                    FROM ItemApplicationEntry Where OutboundItemEntryNo_<>0")
                
                #test="(SELECT * FROM "+DBET+".tempnotdonelot ) AS itemapp"
                ItemApp1.persist()
                ItemApp1.write.jdbc(url=postgresUrl, table="Inventory.iap", mode=owmode, properties=PostgresDbInfo.props)
                ItemApp1.registerTempTable("ItemApp1")
                NotDoneLot1.registerTempTable("NotDoneLot1")
                # print(NotDoneLot1.count())
                # exit()
            #ItemApp1.write.jdbc(url=postgresUrl, table="Inventory.iap", mode=owmode, properties=PostgresDbInfo.props)
            #print(NotDoneLot1.count())
            #exit()
            #vRows1=0
            vRows=1
            vLot=2
            vTrnType='Out'
            var=1
            while var>0:
                vPrevLot=vLot-1 #Case When TrfFrom<>0 Then ILEInbound  Else `Item Ledger Entry No_` End AS Link_In1,\
                if vTrnType=='In':
                    print("In Section")
                    #sys.exit()
                    Ref = spark.sql("SELECT `ILEInbound` AS Link_In1,\
                      `RefNo`,%d"%vLot+" AS LotNo,\
                      'Mapped' AS RefFlag \
                      From ItemApp%d"%vRows).dropDuplicates(['Link_In1'])
                    Ref1 = spark.sql("SELECT `ItemLedgerEntryNo_` AS Link_In1,\
                      `RefNo`,%d"%vLot+" AS LotNo,\
                      'Mapped' AS RefFlag \
                      From ItemApp%d"%vRows).dropDuplicates(['Link_In1'])
                    #exec("Refjoin=[NotDoneLot%d"%vRows+".Link_In == Ref.Link_In1]")
                    #exec("Refjoin1=[NotDoneLot%d"%vRows+".Link_In == Ref1.Link_In1]")
                    exec("TempNotDoneLots0%d"%vLot+"= NotDoneLot%d"%vRows+".filter("+chr(34)+"TrfFrom!=0"+chr(34)+")\
                    .join(Ref,[NotDoneLot%d"%vRows+".Link_In == Ref.Link_In1],'left').drop('Link_In1')",globals())
                    exec("TempNotDoneLots1%d"%vLot+"= NotDoneLot%d"%vRows+".filter("+chr(34)+"TrfFrom=0"+chr(34)+")\
                    .join(Ref1,[NotDoneLot%d"%vRows+".Link_In == Ref1.Link_In1],'left').drop('Link_In1')",globals())
                    exec("TempNotDoneLot%d"%vLot+"=TempNotDoneLots1%d"%vLot+".unionAll(TempNotDoneLots0%d"%vLot+")", globals())
                else:
                    print("Out Section")
                    Ref = spark.sql("SELECT  `ItemLedgerEntryNo_`  AS Link_Out1,\
                     `RefNo`,%d"%vLot+" AS LotNo,\
                      'Mapped' AS RefFlag \
                      From ItemApp%d"%vRows).dropDuplicates(['Link_Out1'])
                    #exec("Refjoin=[NotDoneLot%d"%vRows+".Link_Out == Ref.Link_Out1]", locals1)#R
                    exec("TempNotDoneLot%d"%vLot+"= NotDoneLot%d"%vRows+".join(Ref,[NotDoneLot%d"%vRows+".Link_Out == Ref.Link_Out1],'left').drop('Link_Out1')",globals())
                    
            ##################################### End of Loop ###############################################
                print(vRows,vLot)
                vRows=vRows+1
                vLot=vLot+1
                print(vRows,vLot)
                #sys.exit()
                if vRows%5==0:
                    exec("TempNotDoneLot%d"%vRows+".write.mode(owmode).save(STAGE2_PATH+"+chr(34)+"/tempnotdonelot"+chr(34)+")")
                    exec("TempNotDoneLot%d"%vRows+"=sqlCtx.read.parquet(STAGE2_PATH+"+chr(34)+"/tempnotdonelot"+chr(34)+")",globals())
                exec("TempNotDoneLot%d"%vRows+".cache()")
                exec("TempNotDoneLot%d"%vRows+".registerTempTable("+chr(34)+"TempNotDoneLot%d"%vRows+chr(34)+")")
                NotDoneLot=spark.sql("SELECT * From TempNotDoneLot%d"%vRows+" where `RefNo` IS NULL")
                #exec("NotDoneLot1 = TempNotDoneLot%d"%vRows+".filter(col("+chr(34)+"RefNo"+chr(34)+").isNull())")
                NotDoneLot = NotDoneLot.drop('RefNo').drop('RefFlag').drop('LotNo')
                exec("NotDoneLot%d"%vRows+"=NotDoneLot",globals())
                exec("NotDoneLot%d"%vRows+".registerTempTable("+chr(34)+"NotDoneLot%d"%vRows+chr(34)+")")
                exec("ItemApp%d"%vRows+"=TempNotDoneLot%d"%vRows+".filter(TempNotDoneLot%d"%vRows+".RefNo>0)\
                .drop('Link_In').drop('Link_Out')",globals())
                exec("var=NotDoneLot%d"%vRows+".count()",globals())
                exec("vRows1=ItemApp%d"%vRows+".count()", globals())
                print("Count",vRows1)
                #exit()
                if vRows1>0:
                    print("Continue")
                else:
                    print("Break Loop Section")
                    exec("TempNotDoneLot%d"%vRows+"=TempNotDoneLot%d"%vRows+".na.fill({'RefNo': 0, 'LotNo': %d"%vRows+", 'RefFlag': 'Not Mapped'})", globals())
                    #exec("TempNotDoneLot%d"%vRows+"=TempNotDoneLot%d"%vRows+".withColumn('EntityName',lit("+EntityName+"))")
                    exec("TempNotDoneLot%d"%vRows+".drop('Link_In').drop('Link_Out')\
                       .write.jdbc(url=postgresUrl, table="+chr(34)+"Inventory.iap"+chr(34)+", properties=PostgresDbInfo.props, mode=apmode)")
                    break
                exec("ItemApp%d"%vRows+".registerTempTable("+chr(34)+"ItemApp%d"%vRows+chr(34)+")")
                #exec("ItemApp%d"%vRows+"=ItemApp%d"%vRows+".withColumn('EntityName',lit("+EntityName+"))")
                exec("ItemApp%d"%vRows+".write.jdbc(url=postgresUrl, table="+chr(34)+"Inventory.iap"+chr(34)+", properties=PostgresDbInfo.props, mode=apmode)")
                if vPrevLot>1:
                    exec("TempNotDoneLot%d"%vPrevLot+".unpersist()")
                else:
                    ItemApp1.unpersist()
                if vTrnType=='In':
                    vTrnType='Out'
                else:
                    vTrnType='In'
            ItemApp1.unpersist()
            sqlCtx.clearCache()
            count = count + 1
            
            # #establishing the connection
            # conn = psycopg2.connect(
            # database=entityLocation, user=PostgresDbInfo.props["user"], password=PostgresDbInfo.props["password"], host=PostgresDbInfo.Host, port= PostgresDbInfo.Port)
            # #Setting auto commit false
            # conn.autocommit = True
            # #Creating a cursor object using the cursor() method
            # cursor = conn.cursor()
            # cursor.execute("DROP TABLE inventory.iap_readonly")
            # print(" Read Only Table dropped... ")
            # cursor.execute('''SELECT "EntryNo_", "ItemLedgerEntryNo_", "Inbound", "Outbound", "Quantity", "TrfFrom", "ILEInbound", "OutboundTrf", "PostingDate"+ interval '1' HOUR * 5.5,"RefNo", "LotNo", "RefFlag"  into inventory.iap_readonly FROM inventory.iap''')
            # print("Read Only Table created... ")

            # #Commit your changes in the database
            # conn.commit()
            # #Closing the connection
            # conn.close()

        
            logger.endExecution()

            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"

            log_dict = logger.getSuccessLoggedRecord("Inventory.IAP", DBName, EntityName, 0, 0, IDEorBatch)
            log_df = spark.createDataFrame(log_dict, logger.getSchema())
            log_df.write.jdbc(url=PostgresDbInfo.logsDbUrl, table="logtable", mode='append', properties=PostgresDbInfo.props)

    except Exception as ex:
        exc_type,exc_value,exc_traceback=sys.exc_info()
        print("Error:",ex)
        print("type - "+str(exc_type))
        print("File - "+exc_traceback.tb_frame.f_code.co_filename)
        print("Error Line No. - "+str(exc_traceback.tb_lineno))
        #exec("TempNotDoneLot%d"%vLot+".filter(TempNotDoneLot%d"%vLot+".ItemLedgerEntryNo_=='1759251').show()")
        #sys.exit()
        logger.endExecution()

        try:
            IDEorBatch = sys.argv[1]
        except Exception as e :
            IDEorBatch = "IDLE"

        log_dict = logger.getErrorLoggedRecord('Inventory.IAP', DBName, EntityName, str(ex), str(exc_traceback.tb_lineno), IDEorBatch)
        log_df = spark.createDataFrame(log_dict, logger.getSchema())
        log_df.write.jdbc(url=PostgresDbInfo.logsDbUrl, table="logtable", mode='append', properties=PostgresDbInfo.props)

    print('inventory_IAP completed: ' + str((dt.datetime.now()-st).total_seconds()))

if __name__ == "__main__":
    sqlCtx, spark = getSparkConfig4g(SPARK_MASTER, "Stage2:IAP")
    inventory_IAP(sqlCtx, spark)
