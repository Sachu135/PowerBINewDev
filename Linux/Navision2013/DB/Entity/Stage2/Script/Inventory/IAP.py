
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from datetime import  date
from pyspark.sql.functions import col,concat,concat_ws,when,lit,count,split
import pyspark.sql.functions as F
import datetime,time,sys
from pyspark.sql.types import *
from builtins import str
import os
from os.path import dirname, join, abspath
import datetime as dt
st = dt.datetime.now()
sys.path.insert(0,'../../../..')
mode = 'overwrite'
apmode = 'append'
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
STAGE1_Configurator_Path=HDFS_PATH+DIR_PATH+"/" +DBName+"/" +EntityName+"/" +"Stage1/ConfiguratorData/"
STAGE1_PATH=HDFS_PATH+DIR_PATH+"/" +DBName+"/" +EntityName+"/" +"Stage1/ParquetData"
Stage2_Path =HDFS_PATH+DIR_PATH+"/" +DBName+"/" +EntityName+"/" +"Stage2/ParquetData/Inventory"
sqlCtx,spark=getSparkConfig(SPARK_MASTER, "Stage2:Inventory-IAP")
def inventory_IAP():
    cdate = datetime.datetime.now().strftime('%Y-%m-%d')
    for dbe in config["DbEntities"]:
        if dbe['ActiveInactive']=='true' and  dbe['Location']==DBEntity:
            CompanyName=dbe['Name']
            CompanyName=CompanyName.replace(" ","")
    import psycopg2  
    try:
        logger = Logger()
        count = 1
        flag=2
        try:
            Query_IAPCheck = "(SELECT * FROM Inventory.iap_readonly) AS COA"
            IAPCheck = spark.read.format("jdbc").options(url=PostgresDbInfo.PostgresUrl, dbtable=Query_IAPCheck,\
                                                user=PostgresDbInfo.props["user"],password=PostgresDbInfo.props["password"],driver= PostgresDbInfo.props["driver"]).load()
            flag=1
        except:
            flag=0
        IAE = spark.read.format("delta").load(STAGE1_PATH+"/Item Application Entry")
        IAE.registerTempTable("ItemApplicationEntry")
        global ItemApp1,NotDoneLot1,Ref,NotDoneLot,Ref1
        if flag==1:
            iae = IAE.\
                    withColumn('ILEInbound',concat(col('ItemLedgerEntryNo_'),lit('|'),col('InboundItemEntryNo_'))).\
                    withColumn('OutboundTrf',concat(col('OutboundItemEntryNo_'),lit('|'),col('Transferred-fromEntryNo_')))
            iap = IAPCheck.select(col('EntryNo_'),\
                                col('RefNo'),\
                                col('LotNo')).filter(col('RefNo')!=0).filter(col('RefFlag')=='Mapped')
            iaejoiniap = iae.join(iap,on=['EntryNo_'],how='left')
            iaejoiniap = iaejoiniap.na.fill(0)
            iaejoiniap.persist()
            mapped = iaejoiniap.filter(iaejoiniap['RefNo']!=0)
            unmapped = iaejoiniap.filter(iaejoiniap['RefNo']==0)
            done = unmapped.filter(unmapped['OutboundItemEntryNo_']==0).\
                withColumn('RefNo',col('ItemLedgerEntryNo_')).\
                withColumn('LotNo',lit(1))
               
            notdone = unmapped.filter(unmapped['OutboundItemEntryNo_']!=0)
            mapped = mapped.union(done)
    
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
            ItemApp1.cache()
            ItemApp1.count()
            ItemApp1.registerTempTable("ItemApp1")
            ItemApp1.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="Inventory.iap", mode=owmode, properties=PostgresDbInfo.props)
            NotDoneLot1.persist()   
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
            
            ItemApp1.cache()
            ItemApp1.count()
            ItemApp1.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="Inventory.iap", mode=owmode, properties=PostgresDbInfo.props)
            ItemApp1.registerTempTable("ItemApp1")
            NotDoneLot1.registerTempTable("NotDoneLot1")
            
        vRows=1
        vLot=2
        vTrnType='Out'
        var=1
        while var>0:
            vPrevLot=vLot-1 
            if vTrnType=='In':
                Ref = spark.sql("SELECT `ILEInbound` AS Link_In1,\
                  `RefNo`,%d"%vLot+" AS LotNo,\
                  'Mapped' AS RefFlag \
                  From ItemApp%d"%vRows).dropDuplicates(['Link_In1'])
                Ref1 = spark.sql("SELECT `ItemLedgerEntryNo_` AS Link_In1,\
                  `RefNo`,%d"%vLot+" AS LotNo,\
                  'Mapped' AS RefFlag \
                  From ItemApp%d"%vRows).dropDuplicates(['Link_In1'])
                exec("TempNotDoneLots0%d"%vLot+"= NotDoneLot%d"%vRows+".filter("+chr(34)+"TrfFrom!=0"+chr(34)+")\
                .join(Ref,[NotDoneLot%d"%vRows+".Link_In == Ref.Link_In1],'left').drop('Link_In1')",globals())
                exec("TempNotDoneLots1%d"%vLot+"= NotDoneLot%d"%vRows+".filter("+chr(34)+"TrfFrom=0"+chr(34)+")\
                .join(Ref1,[NotDoneLot%d"%vRows+".Link_In == Ref1.Link_In1],'left').drop('Link_In1')",globals())
                exec("TempNotDoneLot%d"%vLot+"=TempNotDoneLots1%d"%vLot+".unionAll(TempNotDoneLots0%d"%vLot+")", globals())
            else:
               
                Ref = spark.sql("SELECT  `ItemLedgerEntryNo_`  AS Link_Out1,\
                 `RefNo`,%d"%vLot+" AS LotNo,\
                  'Mapped' AS RefFlag \
                  From ItemApp%d"%vRows).dropDuplicates(['Link_Out1'])
                
                exec("TempNotDoneLot%d"%vLot+"= NotDoneLot%d"%vRows+".join(Ref,[NotDoneLot%d"%vRows+".Link_Out == Ref.Link_Out1],'left').drop('Link_Out1')",globals())
            vRows=vRows+1
            vLot=vLot+1
            if vRows%5==0:
                exec("TempNotDoneLot%d"%vRows+".write.mode(owmode).save(Stage2_Path+"+chr(34)+"/tempnotdonelot"+chr(34)+")")
                exec("TempNotDoneLot%d"%vRows+"=spark.read.parquet(Stage2_Path+"+chr(34)+"/tempnotdonelot"+chr(34)+")",globals())
            exec("TempNotDoneLot%d"%vRows+".cache()")
            exec("TempNotDoneLot%d"%vRows+".registerTempTable("+chr(34)+"TempNotDoneLot%d"%vRows+chr(34)+")")
            NotDoneLot=spark.sql("SELECT * From TempNotDoneLot%d"%vRows+" where `RefNo` IS NULL")
            NotDoneLot = NotDoneLot.drop('RefNo').drop('RefFlag').drop('LotNo')
            exec("NotDoneLot%d"%vRows+"=NotDoneLot",globals())
            exec("NotDoneLot%d"%vRows+".registerTempTable("+chr(34)+"NotDoneLot%d"%vRows+chr(34)+")")
            exec("ItemApp%d"%vRows+"=TempNotDoneLot%d"%vRows+".filter(TempNotDoneLot%d"%vRows+".RefNo>0)\
            .drop('Link_In').drop('Link_Out')",globals())
            exec("var=NotDoneLot%d"%vRows+".count()",globals())
            exec("vRows1=ItemApp%d"%vRows+".count()", globals())
            print("Count",vRows1)
            if vRows1>0:
                print("Continue")
            else:
                print("Break Loop Section")
                exec("TempNotDoneLot%d"%vRows+"=TempNotDoneLot%d"%vRows+".na.fill({'RefNo': 0, 'LotNo': %d"%vRows+", 'RefFlag': 'Not Mapped'})", globals())
                exec("TempNotDoneLot%d"%vRows+".drop('Link_In').drop('Link_Out')\
                   .write.jdbc(url=PostgresDbInfo.PostgresUrl, table="+chr(34)+"Inventory.iap"+chr(34)+", properties=PostgresDbInfo.props, mode=apmode)")
                break
            exec("ItemApp%d"%vRows+".registerTempTable("+chr(34)+"ItemApp%d"%vRows+chr(34)+")")
            exec("ItemApp%d"%vRows+".write.jdbc(url=PostgresDbInfo.PostgresUrl, table="+chr(34)+"Inventory.iap"+chr(34)+", properties=PostgresDbInfo.props, mode=apmode)")
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
        conn = psycopg2.connect(
        database=PostgresDbInfo.PostgresDB, user=PostgresDbInfo.props["user"], password=PostgresDbInfo.props["password"], host=PostgresDbInfo.Host, port= PostgresDbInfo.Port)
        conn.autocommit = True
        cursor = conn.cursor()
        try:
            cursor.execute("DROP TABLE inventory.iap_readonly")
            print("Read Only Table dropped... ")
            cursor.execute('''SELECT "EntryNo_", "ItemLedgerEntryNo_", "Inbound", "Outbound", "Quantity", "TrfFrom", "ILEInbound", "OutboundTrf", TO_CHAR("PostingDate",'YYYY-MM-DD') as "PostingDate","RefNo", "LotNo", "RefFlag"  into inventory.iap_readonly FROM inventory.iap''')
            print("Read Only Table created... ")
        except:
            print("iap_readonly table not found...")
            cursor.execute('''SELECT "EntryNo_", "ItemLedgerEntryNo_", "Inbound", "Outbound", "Quantity", "TrfFrom", "ILEInbound", "OutboundTrf", TO_CHAR("PostingDate",'YYYY-MM-DD') as "PostingDate","RefNo", "LotNo", "RefFlag"  into inventory.iap_readonly FROM inventory.iap''')
            print("Read Only Table created... ")
        conn.commit()
        conn.close()
        logger.endExecution()
        try:
            IDEorBatch = sys.argv[1]
        except Exception as e :
            IDEorBatch = "IDLE"
    
        log_dict = logger.getSuccessLoggedRecord("Inventory.IAP", DBName, EntityName, 0, 0, IDEorBatch)
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
        os.system("spark-submit "+Kockpit_Path+"/Email.py 1 IAP '"+CompanyName+"' "+DBEntity+" "+str(exc_traceback.tb_lineno)+" ")
        log_dict = logger.getErrorLoggedRecord('Inventory.IAP', DBName, EntityName, str(ex), str(exc_traceback.tb_lineno), IDEorBatch)
        log_df = spark.createDataFrame(log_dict, logger.getSchema())
        log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)
    print('Inventory_IAP completed: ' + str((dt.datetime.now()-st).total_seconds()))
if __name__ == "__main__":
    inventory_IAP()    
    
