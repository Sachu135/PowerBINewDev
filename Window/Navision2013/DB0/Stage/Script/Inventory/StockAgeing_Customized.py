'''
Created on 04 March 2021
@author: Prashant
'''
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext, StorageLevel
from pyspark.sql import SQLContext
import csv, io,re,keyring,os,datetime
from datetime import timedelta, date
from pyspark.sql.functions import col,max as max_,concat,concat_ws,year,when,to_date,lit,last_day,datediff
import datetime,time,sys,calendar
from pyspark.sql.types import *
import re
from builtins import str
import pandas as pd
import traceback
import os,sys,subprocess
from os.path import dirname, join, abspath
from distutils.command.check import check
import datetime as dt

helpersDir = '/home/padmin/KockpitStudio'
sys.path.insert(0, helpersDir)
from ConfigurationFiles.AppConfig import *
from Helpers.Constants import *
from Helpers.udf import *

Datelog = datetime.datetime.now().strftime('%Y-%m-%d')
start_time = datetime.datetime.now()
stime = start_time.strftime('%H:%M:%S')
today = datetime.date.today().strftime("%Y-%m-%d")

def inventory_StockAgeing_Customized(sqlCtx, spark):
    st = dt.datetime.now()
    logger = Logger()

    try:
    
        VE_Entity = next (table for table in config["TablesToIngest"] if table["Table"] == "Value Entry")
        trhEntity = next (table for table in config["TablesToIngest"] if table["Table"] == "Transfer Receipt Header")
        trlEntity = next (table for table in config["TablesToIngest"] if table["Table"] == "Transfer Receipt Line")    
        pilEntity = next (table for table in config["TablesToIngest"] if table["Table"] == "Purch_ Inv_ Line") 
        pihEntity = next (table for table in config["TablesToIngest"] if table["Table"] == "Purch_ Inv_ Header")
        ilEntity = next (table for table in config["TablesToIngest"] if table["Table"] == "Item Ledger Entry")
        
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

            Query_FieldSelection="(SELECT *\
                                FROM "+chr(34)+"tblFieldSelection"+chr(34)+") AS df"
            table = spark.read.format("jdbc").options(url=PostgresDbInfo.Configurl, dbtable=Query_FieldSelection,\
                                    user=PostgresDbInfo.props["user"],password=PostgresDbInfo.props["password"],driver= PostgresDbInfo.props["driver"]).load()
            table = table.filter(table['Flag'] == 1).filter(table['TableName'] == 'Value Inventory')
            FieldName = table.select("FieldType").collect()[0]["FieldType"]
            FieldName = re.sub("[\s+]",'',FieldName)
        
            trl =ToDFWitoutPrefix(sqlCtx, hdfspath, trlEntity,True)
            trl =trl.select('DocumentNo_','LineNo_','SmartDeskTicketIdNo_')\
                    .withColumnRenamed('SmartDeskTicketIdNo_','ILE_TicketID_No')
            trl = trl.withColumn('key',concat_ws('-',trl['DocumentNo_'],trl['LineNo_']))
            trl = trl.drop('DocumentNo_','LineNo_')

            trh = ToDFWitoutPrefix(sqlCtx, hdfspath, trhEntity,True)
            trh = trh.select('No_','PartyRef_No_','PartyType','PartyDescription','Transfer-toCity')\
                        .withColumnRenamed('No_','DocumentNo_').withColumnRenamed('Transfer-toCity','TransferToCity')

            ve = ToDFWitoutPrefix(sqlCtx, hdfspath, VE_Entity,True)
            ve = ve.withColumn("PostingDate",to_date(ve.PostingDate))
            ve = ve.withColumnRenamed("DocumentNo_","Document_No").withColumnRenamed("DocumentLineNo_","VE_Line_No")\
                    .withColumnRenamed("GlobalDimension2Code","Branch").withColumnRenamed("GlobalDimension11Code","OT_BRANCH")\
                    .withColumnRenamed("GlobalDimension1Code","SBU_Code").withColumnRenamed("GlobalDimension14Code","BU_Code")\
                    .withColumnRenamed("SourceNo_","Source_No").withColumnRenamed("SourceType","Source_Type")\
                    .withColumnRenamed("GlobalDimension13Code","GD13C")

            DSE = sqlCtx.read.parquet(hdfspath + "/DSE").drop("DBName","EntityName")
            ve = ve.join(DSE,"DimensionSetID",'left')
            ve=ve.withColumn('DBName',lit(DBName))\
                    .withColumn('EntityName',lit(EntityName))
            ve = ve.groupby("ItemLedgerEntryNo_","EntryType","PostingDate","ItemNo_","SBU_Code","BU_Code","GD13C","LocationCode","DBName","EntityName",\
                    "Document_No","Branch","Source_No","Source_Type","VE_Line_No","OT_BRANCH","VariantCode","Link_TARGETPROD")\
                    .agg({"CostPostedtoG_L":"sum",FieldName:"sum","ValuedQuantity":"avg"})\
                    .drop("CostPostedtoG_L",FieldName,  "ValuedQuantity")

            ve = RENAME(ve,{"ItemLedgerEntryNo_":"ILENo","sum(CostPostedtoG_L)":"VEValue","sum("+FieldName+")":"VEQuantity","avg(ValuedQuantity)":"ValuedQuantity"})
            ItemInv = ve.withColumn("LinkItem",when(ve.ItemNo_=='',lit("NA")).otherwise(ve.ItemNo_)).drop("ItemNo_")\
                .withColumn("Monthend",last_day(ve.PostingDate))\
                .withColumn("LocationCode",when(ve.LocationCode=='',lit("NA")).otherwise(ve.LocationCode))\
                .withColumn("Vendor_No",when(ve.Source_Type==2,ve.Source_No).otherwise(lit("NA")))\
                .withColumn("Link_VPO_Key",concat_ws('|','Document_No','VE_Line_No'))
            ItemInv  = ItemInv.drop("EntryType").drop("VariantCode")#.drop("LocationCode")
            
            IAP_query="(SELECT  "+chr(34)+"ItemLedgerEntryNo_"+chr(34)+" AS "+chr(34)+"ILENo1"+chr(34)+",\
                    "+chr(34)+"Quantity"+chr(34)+" AS "+chr(34)+"IAEQuantity"+chr(34)+",\
                    "+chr(34)+"RefNo"+chr(34)+" \
                    FROM inventory.iap) AS IAP"
            IAP0 = sqlCtx.read.format("jdbc").options(url=postgresUrl, dbtable=IAP_query,\
                        user=PostgresDbInfo.props["user"],password=PostgresDbInfo.props["password"],\
                        driver= PostgresDbInfo.props["driver"],partitionColumn=chr(34)+"ILENo1"+chr(34),\
                        lowerBound=0, upperBound=1791335, numPartitions=128).load()

            IAP1=IAP0.groupBy('ILENo1').agg({'RefNo': 'count'})
            IAP1 = RENAME(IAP1,{"ILENo1":"ILENo2","count(RefNo)":"RefNoCount"})
            CountJoin=[IAP0.ILENo1==IAP1.ILENo2]
            #IAP=IAP0.join(IAP1,CountJoin,'left').drop('ILENo2')
            IAP = LJOIN(IAP0,IAP1,CountJoin)
            IAP = IAP.drop("ILENo2")

            Cond=[ItemInv.ILENo == IAP.ILENo1]
            #ItemInv1=ItemInv.join(IAP,Cond,'left').drop('ILENo1').na.fill({'RefNoCount':0,'RefNo':0})
            ItemInv1 = LJOIN(ItemInv,IAP,Cond)
            ItemInv1 = ItemInv1.drop("ILENo1").na.fill({'RefNoCount':0,'RefNo':0})
            
            ItemInv2 = ItemInv1.withColumn("RefNo1",when(ItemInv1.RefNo==0,ItemInv1.ILENo).otherwise(ItemInv1.RefNo))\
                    .withColumn("StockValue",when(ItemInv1.RefNoCount<2,ItemInv1.VEValue)\
                    .otherwise(when(ItemInv1.VEQuantity!=0,((ItemInv1.VEValue)/(ItemInv1.VEQuantity/ItemInv1.IAEQuantity))).otherwise((ItemInv1.VEValue)/(ItemInv1.ValuedQuantity/ItemInv1.IAEQuantity))))\
                    .withColumn("StockQuantity",when(ItemInv1.RefNoCount<2,ItemInv1.VEQuantity).otherwise(when(ItemInv1.VEQuantity!=0,ItemInv1.IAEQuantity).otherwise(lit(0))))\
                    .drop("RefNo")
            ItemInv2 = RENAME(ItemInv2,{"RefNo1":"RefNo"})

            InDates = ItemInv1.select("ILENo","PostingDate")
            InDates = InDates.groupby("ILENo").agg({"PostingDate":"min"})
            InDates = RENAME(InDates,{"ILENo":"RefNo2","min(PostingDate)":"InDate"})
        
            CondMinDate=[ItemInv2.RefNo == InDates.RefNo2]
            ItemInv3 = LJOIN(ItemInv2,InDates,CondMinDate)
            
            ItemInv3 = ItemInv3.drop("RefNo2").drop("PostingDate")
            
            ItemInv3 = RENAME(ItemInv3,{"LocationCode":"LinkLocation","RefNo":"RefILENo"})
            ItemInv3 = ItemInv3.na.fill({'LinkLocation':'NA','LinkItem':'NA'})

            ItemInv3 = ItemInv3.withColumn("LinkLocationKey",concat_ws("|",ItemInv3.DBName,ItemInv3.EntityName,ItemInv3.LinkLocation))\
                    .withColumn("LinkItemKey",concat_ws("|",ItemInv3.DBName,ItemInv3.EntityName,ItemInv3.LinkItem))\
                    .withColumn("StockAge", datediff(lit(today),col("InDate")))
                    
            ItemInv3=ItemInv3.na.fill({'StockAge':0})
            ItemInv3.persist(StorageLevel.MEMORY_AND_DISK)
            list = ItemInv3.select("StockAge").distinct().rdd.flatMap(lambda x: x).collect()

            Query_InvBucket="(SELECT *\
                            FROM "+chr(34)+"tblInventoryBucket"+chr(34)+") AS df"
            InvBucket = spark.read.format("jdbc").options(url=PostgresDbInfo.Configurl, dbtable=Query_InvBucket,\
                                user=PostgresDbInfo.props["user"],password=PostgresDbInfo.props["password"],driver= PostgresDbInfo.props["driver"]).load()
            
            Bucket = InvBucketBucket = InvBucket.withColumnRenamed("Lower Limit","LowerLimit")\
                                .withColumnRenamed("Upper Limit","UpperLimit")\
                                .withColumnRenamed("Bucket Name","BucketName")\
                                .withColumnRenamed("Bucket Sort","BucketSort")
            Bucket = Bucket.select("LowerLimit","UpperLimit","BucketName","BucketSort")
            Bucket = Bucket.collect()
            TempBuckets = BUCKET(list,Bucket,sqlCtx)
            CondBucket = [ItemInv3.StockAge == TempBuckets.NOD]
            ItemInv3 = LJOIN(ItemInv3,TempBuckets,CondBucket)

            pil = ToDFWitoutPrefix(sqlCtx, hdfspath, pilEntity, True)    
            pil = pil.na.fill({'No_':'NA'})
            pil = pil.withColumnRenamed("Type","PIL_Type")
            pil = pil.withColumn("Link_VPO_Key1",concat(col('DocumentNo_'),lit('|'),col('LineNo_')))
            pil = pil.drop("DBName").drop("EntityName").drop("PostingDate").drop("EntryType").drop("timestamp").drop("LocationCode").drop("Quantity")
            pih = ToDFWitoutPrefix(sqlCtx, hdfspath, pihEntity, True)
            pih = pih.withColumnRenamed("No_","No_1")
            pih = pih.withColumn('DBName',lit(DBName))\
                    .withColumn('EntityName',lit(EntityName))
            pih = pih.withColumn("LinkPurchaserCodeKey",concat_ws("|",pih.DBName,pih.EntityName,pih.PurchaserCode))
            pih = pih.withColumn("LinkCustomerKey",concat_ws("|",pih.DBName,pih.EntityName,pih["Sell-toCustomerNo_"]))
            pih = pih.drop("DBName").drop("EntityName").drop("DimensionSetID")
            pih = pih.withColumnRenamed("OrderNo_","VPO No").drop("EntryType").drop("timestamp").drop("LocationCode").drop("Quantity")
            
            CountJoin=[ItemInv3.Link_VPO_Key==pil.Link_VPO_Key1]
            ItemInv3=ItemInv3.join(pil,CountJoin,'left').drop('Link_VPO_Key1')
            
            ########################################NOTDONE#######
        
            CountJoin=[ItemInv3.Document_No==pih.No_1]
            ItemInv3=ItemInv3.join(pih,CountJoin,'left')   
            ItemInv3 = ItemInv3.withColumn("LinkBranchKey",concat_ws("|",ItemInv3.DBName,ItemInv3.EntityName,ItemInv3.Branch))
            ItemInv3 = ItemInv3.withColumn("LinkOT_BranchKey",concat_ws("|",ItemInv3.DBName,ItemInv3.EntityName,ItemInv3.OT_BRANCH))
            ItemInv3 = ItemInv3.withColumn("LinkSBUKey",concat_ws("|",ItemInv3.DBName,ItemInv3.EntityName,ItemInv3.SBU_Code))
            ItemInv3 = ItemInv3.withColumn("LinkBUKey",concat_ws("|",ItemInv3.DBName,ItemInv3.EntityName,ItemInv3.BU_Code))
            ItemInv3 = ItemInv3.withColumn("LinkDateKey",concat_ws("|",ItemInv3.DBName,ItemInv3.EntityName,ItemInv3.PostingDate))
            ItemInv3 = ItemInv3.withColumn("Link_TARGETPRODKey",concat_ws("|",ItemInv3.DBName,ItemInv3.EntityName,ItemInv3.Link_TARGETPROD))
            
            #ItemInv3.cache()
            
            ILE = ToDFWitoutPrefix(sqlCtx, hdfspath, ilEntity, True)
            ile = ILE.select('EntryNo_','DocumentNo_','SerialNo_','LotNo_','DCNo')\
                    .withColumnRenamed('EntryNo_','ILENo')\
                    .withColumnRenamed('SerialNo_','ILE_Serial_No')\
                    .withColumnRenamed('LotNo_','ILE_Lot_No')\
                    .withColumnRenamed('DCNo','ILE_DC_No')
            ile_joiner = ile.join(trh,'DocumentNo_','left')
            ile_joiner = ile_joiner.withColumnRenamed('DocumentNo_','ILE_Document_No_latest')
            
            ILE = ILE.withColumnRenamed("DocumentNo_","ILE_DocumentNo_").withColumnRenamed("PostingDate","ILE_PostingDate")
            ILE = ILE.drop("Entry Type").drop("timestamp").drop("LocationCode").drop("Quantity").drop("DimensionSetID").drop("SmartDeskTicketIdNo_")

            CountJoin=[ILE.EntryNo_==ItemInv3.ILENo]
            ItemInv3=ItemInv3.join(ILE,CountJoin,'left')
            ItemInv3.persist(StorageLevel.MEMORY_AND_DISK)
            ''' Flowing VPO no, Customer Name , Sales person etc which always present in PR document No '''
            ItemInv3_grpby = ItemInv3.select(["RefILENo","Document_No","DocumentNo_","LinkPurchaserCodeKey","LinkCustomerKey","PIL_Type",\
                                            "Source_Type","No_" ,"VPO No","Vendor_No","VPOSONo_","VendorInvoiceNo_","LinkLocationKey"])
        
            ItemInv3_grpby = ItemInv3_grpby.filter(ItemInv3_grpby["Source_Type"]==2)#.filter(ItemInv3_grpby["PIL_Type"]==2)
            ItemInv3_grpby = ItemInv3_grpby.filter(ItemInv3_grpby.Document_No.like('PR%')).filter(ItemInv3_grpby.No_.like('IT%'))
            #ItemInv3_grpby = ItemInv3_grpby.withColumn("Link_Self_Join1",concat(col('LinkLocationKey'),col('RefILENo')))
            ItemInv3_grpby = ItemInv3_grpby.withColumn("Link_Self_Join1",col('RefILENo'))
            
            ItemInv3_grpby = ItemInv3_grpby.drop('RefILENo').drop('No_').drop('LinkLocationKey').drop("Document_No").drop("PIL_Type").drop("Source_Type")

            ItemInv3_grpby = ItemInv3_grpby.withColumnRenamed("LinkPurchaserCodeKey","LinkPurchaserCodeKey1").withColumnRenamed("LinkCustomerKey","LinkCustomerKey1")\
                                        .withColumnRenamed("VPO No","VPO No1").drop("Vendor_No","Vendor_No1").withColumnRenamed("VPOSONo_","VPOSONo_1")\
                                        .withColumnRenamed("VendorInvoiceNo_","VendorInvoiceNo_1").withColumnRenamed("DocumentNo_","DocumentNo_1")
            ItemInv3 = ItemInv3.withColumn("Link_Self_Join",col('RefILENo'))
            CountJoin=[ItemInv3_grpby.Link_Self_Join1==ItemInv3.Link_Self_Join]
            ItemInv3=ItemInv3.join(ItemInv3_grpby,CountJoin,'left')
            
            ##########################NOTDONE##################################################
            #There are some Document No. where Only Customer ID is blank, so we extract customer code from GLobal Dimension 13 in ValueEntry
            ItemInv3 = ItemInv3.withColumnRenamed("LinkCustomerKey1","OrgCustKey")
            ItemInv3 = ItemInv3.drop("LinkCustomerKey1")
            #ItemInv3.select("OrgCustKey").show()
            ItemInv3 = ItemInv3.withColumn("GDCKey",concat_ws("|",ItemInv3.DBName,ItemInv3.EntityName,ItemInv3["GD13C"])) 
            ItemInv3 = ItemInv3.withColumn("LinkCustomerKey1",when((ItemInv3["Sell-toCustomerNo_"].isNull())|(ItemInv3["Sell-toCustomerNo_"]=='')|(ItemInv3.OrgCustKey=='DB1|E1|C51914'),col("GDCKey")).otherwise(ItemInv3.OrgCustKey))#col("GDCKey")#ItemInv3.OrgCustKey))
            
            #ItemInv3.write.jdbc(url=Postgresurl, table=DBET+".inventory_linux_edited_currentM2", mode=owmode, properties=Postgresprop)
            
            ''' Removing those entries where both quantity and value are 0 so that entries dont repeat after self join'''
            ItemInv3 = ItemInv3.filter((ItemInv3.StockQuantity != 0) |(ItemInv3.StockValue != 0) )
            '''Removing  document no which are of entry type = 0 and replacing them with those of entry type = 0 but quantity and value should be their sum'''
            ItemInv3_grpby = ItemInv3.filter(ItemInv3.StockQuantity!=0)
            ItemInv3_grpby = ItemInv3_grpby.select(["ILENo","Document_No","EntryType"]).distinct()
            
            ItemInv3_grpby = ItemInv3_grpby.filter(ItemInv3_grpby.EntryType==0).withColumnRenamed("ILENo","ILENo2")\
                                        .withColumnRenamed("Document_No","Document_No2").withColumnRenamed("EntryType","EntryType2")
            ItemInv3 = ItemInv3.drop("EntryType2")
            CountJoin=[ItemInv3_grpby.ILENo2==ItemInv3.ILENo]
            ItemInv3=ItemInv3.join(ItemInv3_grpby,CountJoin,'left')
            
            ItemInv3_grpby = ItemInv3.select(["RefILENo","ILENo","StockQuantity","StockValue"])
            
            ItemInv3_grpby = ItemInv3_grpby.groupby("RefILENo","ILENo").sum("StockQuantity","StockValue")
            ItemInv3_grpby = ItemInv3_grpby.withColumnRenamed("sum(StockQuantity)","NewQuantity").withColumnRenamed("sum(StockValue)","NewValue")\
                                            .withColumnRenamed("ILENo","NewILENo").withColumnRenamed("RefILENo","NewRefILENo")

            ItemInv3= ItemInv3.withColumn("Link_SelfJoin2_Key",concat_ws('|','ILENo','RefILENo'))
            ItemInv3_grpby= ItemInv3_grpby.withColumn("Link_SelfJoin2_Key1",concat_ws('|','NewILENo','NewRefILENo'))
            CountJoin=[ItemInv3_grpby.Link_SelfJoin2_Key1==ItemInv3.Link_SelfJoin2_Key]
            ItemInv3=ItemInv3.join(ItemInv3_grpby,CountJoin,'left')
            ItemInv3 = ItemInv3.filter(ItemInv3.EntryType==0)
            ItemInv3 = ItemInv3.withColumn("StockQuantity1",when(ItemInv3.StockQuantity==0,lit(0)).otherwise(lit(1)))
            ''' there are rows which only differ in IAEQuantity,StockQuantity and StockValue'''
            ItemInv3 = ItemInv3.drop("StockQuantity").drop("StockValue").drop("IAEQuantity")
            ItemInv3 = ItemInv3.distinct().filter(ItemInv3.StockQuantity1==1)
            ItemInv3 = ItemInv3.withColumn("PerUnitCost",ItemInv3.NewValue/ItemInv3.NewQuantity)
            ItemInv3 = ItemInv3.withColumnRenamed("NewQuantity","StockQuantity").withColumnRenamed("NewValue","StockValue")
            ItemInv3 = ItemInv3.drop("timestamp")
            ''' same RefILENo has different ILENo but we want to show only those rows where Open=1 but we need  the summation of their Quantity and sum '''
            ItemInv3_grpby = ItemInv3.select(["RefILENo","LinkLocation","StockQuantity","StockValue","Open"])
            ItemInv3_grpby = ItemInv3_grpby.groupby("RefILENo","LinkLocation").sum("StockQuantity","StockValue","Open")
            ItemInv3_grpby = ItemInv3_grpby.withColumnRenamed("sum(StockQuantity)","NewQuantity").withColumnRenamed("sum(StockValue)","NewValue")\
                                            .withColumnRenamed("sum(Open)","NewOpen")
            
            #CountJoin=[ItemInv3_grpby.NewRefILENo==ItemInv3.RefILENo]
            ItemInv3.cache()
            ItemInv3=ItemInv3.join(ItemInv3_grpby,['RefILENo',"LinkLocation"],'left')
            ItemInv3 = ItemInv3.filter(ItemInv3.Open==1)#.filter(ItemInv3.NewOpen>0)
            ItemInv3 = ItemInv3.drop("NewRefILENo").drop("StockQuantity").drop("StockValue")
            
            ItemInv3 = ItemInv3.withColumn("OpenQuantity1",ItemInv3.NewQuantity/ItemInv3.NewOpen)
            ItemInv3 = ItemInv3.withColumn("OpenValue1",ItemInv3.NewValue/ItemInv3.NewOpen)
            ItemInv3 = ItemInv3.withColumnRenamed("OpenQuantity1","StockQuantity").withColumnRenamed("OpenValue1","StockValue")
            ItemInv3 = ItemInv3.join(ile_joiner,'ILENo','left')
            
            ItemInv3 = ItemInv3.withColumn('key',concat_ws('-',ItemInv3['Document_No'],ItemInv3['VE_Line_No']))
            finalDF = ItemInv3.join(trl,'key','left')
            finalDF = RenameDuplicateColumns(finalDF)
            finalDF.cache()
            finalDF.write.jdbc(url=postgresUrl, table="Inventory.StockAgeing_Customized", mode='overwrite', properties=PostgresDbInfo.props)
            logger.endExecution()
                
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"

            log_dict = logger.getSuccessLoggedRecord("Invenory.StockAgeing_Customized", DBName, EntityName, finalDF.count(), len(finalDF.columns), IDEorBatch)
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
        
        log_dict = logger.getErrorLoggedRecord('Invenory.StockAgeing_Customized', DBName, EntityName, str(ex), str(exc_traceback.tb_lineno), IDEorBatch)
        log_df = spark.createDataFrame(log_dict, logger.getSchema())
        log_df.write.jdbc(url=PostgresDbInfo.logsDbUrl, table="logtable", mode='append', properties=PostgresDbInfo.props)
    print('inventory_StockAgeing_Customized completed: ' + str((dt.datetime.now()-st).total_seconds()))
    

if __name__ == "__main__":
    sqlCtx, spark = getSparkConfig(SPARK_MASTER, "Stage2:Inventory_StockAgeing_Customized")
    inventory_StockAgeing_Customized(sqlCtx, spark)