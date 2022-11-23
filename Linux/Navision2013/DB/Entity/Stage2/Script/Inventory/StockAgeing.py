
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext, StorageLevel
from pyspark.sql import SQLContext
import datetime
from datetime import  date
from pyspark.sql.functions import col,max as max_,concat,concat_ws,year,when,to_date,lit,last_day,datediff
import time,sys,calendar
from pyspark.sql.types import *
from builtins import str
import traceback
import re
import os
from os.path import dirname, join, abspath
from distutils.command.check import check
import time
import datetime as dt
st = dt.datetime.now()
from os.path import dirname, join, abspath
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
today = datetime.date.today().strftime("%Y-%m-%d")
STAGE1_Configurator_Path=HDFS_PATH+DIR_PATH+"/" +DBName+"/" +EntityName+"/" +"Stage1/ConfiguratorData/"
STAGE1_PATH=HDFS_PATH+DIR_PATH+"/" +DBName+"/" +EntityName+"/" +"Stage1/ParquetData"
STAGE2_PATH=HDFS_PATH+DIR_PATH+"/" +DBName+"/" +EntityName+"/" +"Stage2/ParquetData"
conf = SparkConf().setMaster(SPARK_MASTER).setAppName("StockAgeing")\
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
import delta
from delta.tables import *
fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
cdate = datetime.datetime.now().strftime('%Y-%m-%d')
for dbe in config["DbEntities"]:
    if dbe['ActiveInactive']=='true' and  dbe['Location']==DBEntity:
        CompanyName=dbe['Name']
        CompanyName=CompanyName.replace(" ","")                                       
        try:
            logger = Logger()
            Datelog = datetime.datetime.now().strftime('%Y-%m-%d')
            CurrentYear = Datelog.split('-')[0]
            CurrentMonth = Datelog.split('-')[1]
            YM = int(CurrentYear+CurrentMonth)
            table=spark.read.format("delta").load(STAGE1_Configurator_Path+"/tblFieldSelection")
            Company =spark.read.format("delta").load(STAGE1_Configurator_Path+"/tblCompanyName")
            ve = spark.read.format("delta").load(STAGE1_PATH+"/Value Entry")
            table = table.filter(table['Flag'] == 1).filter(table['TableName'] == 'Value Inventory')
            FieldName = table.select("FieldType").collect()[0]["FieldType"]
            FieldName = re.sub("[\s+]",'',FieldName)
            purchase_entries = ve.filter(ve["InvoicedQuantity"]!=0) 
            purchase_entries = purchase_entries.select('ItemLedgerEntryNo_','DocumentNo_','PostingDate')\
                                            .withColumnRenamed('ItemLedgerEntryNo_','VE_ILE_NO').withColumnRenamed('DocumentNo_','VEDocumentNo')\
                                            .withColumnRenamed('PostingDate','VEPostingDate')
            
            ve = ve.filter((ve.CapacityLedgerEntryNo_==0) & (year(ve.PostingDate)!=1753))
            ve = ve.withColumn("PostingDate",to_date(ve.PostingDate))
            ve = ve.withColumnRenamed("GlobalDimension1Code","SBU_Code").withColumnRenamed('DocumentNo_','Document_No')
            ve = ve.withColumnRenamed("GlobalDimension14Code","BU_Code")
            ve = ve.groupby("ItemLedgerEntryNo_","SBU_Code","BU_Code","PostingDate","ItemNo_","LocationCode").agg({"CostPostedtoG_L":"sum",FieldName:"sum","ValuedQuantity":"avg"})\
                    .drop("CostPostedtoG_L",FieldName,"ValuedQuantity")
            
            max_postingdate = max(ve.select('PostingDate').rdd.flatMap(lambda x: x).collect())
            
            ve = RENAME(ve,{"ItemLedgerEntryNo_":"ILENo","sum(CostPostedtoG_L)":"VEValue","sum("+FieldName+")":"VEQuantity","avg(ValuedQuantity)":"ValuedQuantity"})
            ItemInv = ve.withColumn("LinkItem",when(ve.ItemNo_=='',lit("NA")).otherwise(ve.ItemNo_)).drop("ItemNo_")\
                .withColumn("Monthend",when(ve.PostingDate>=max_postingdate.replace(day=1),max_postingdate).otherwise(last_day(ve.PostingDate)))\
                .withColumn("LocationCode",when(ve.LocationCode=='',lit("NA")).otherwise(ve.LocationCode))
            
            IAP_query="(SELECT  "+chr(34)+"ItemLedgerEntryNo_"+chr(34)+" AS "+chr(34)+"ILENo1"+chr(34)+",\
                 "+chr(34)+"Quantity"+chr(34)+" AS "+chr(34)+"IAEQuantity"+chr(34)+",\
                 "+chr(34)+"RefNo"+chr(34)+" \
                FROM inventory.iap) AS IAP"
            IAP0 = sqlCtx.read.format("jdbc").options(url=PostgresDbInfo.PostgresUrl, dbtable=IAP_query,\
                      user=PostgresDbInfo.props["user"],password=PostgresDbInfo.props["password"],\
                      driver= PostgresDbInfo.props["driver"],partitionColumn=chr(34)+"ILENo1"+chr(34),\
                      lowerBound=0, upperBound=1791335, numPartitions=128).load()
            
            IAP1=IAP0.groupBy('ILENo1').agg({'RefNo': 'count'})
            IAP1 = RENAME(IAP1,{"ILENo1":"ILENo2","count(RefNo)":"RefNoCount"})
            CountJoin=[IAP0.ILENo1==IAP1.ILENo2]
            IAP = LJOIN(IAP0,IAP1,CountJoin)
            IAP = IAP.drop("ILENo2")
            Cond=[ItemInv.ILENo == IAP.ILENo1]
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
            ItemInv3 = ItemInv3.drop("RefNo2")
            
            Company = Company.filter(col('DBName')==DBName).filter(col('NewCompanyName') == EntityName)
            df = Company.select("StartDate","EndDate")
            Calendar_StartDate = df.select(df.StartDate).collect()[0]["StartDate"]
            Calendar_StartDate = datetime.datetime.strptime(Calendar_StartDate,"%Y-%m-%d").date()
            
            if datetime.date.today().month>int(MnSt)-1:
                    UIStartYr=datetime.date.today().year-int(yr)+1
            else:
                    UIStartYr=datetime.date.today().year-int(yr)
            UIStartDate=datetime.date(UIStartYr,int(MnSt),1)
            UIStartDate=max(Calendar_StartDate,UIStartDate)
            
            Calendar_EndDate_conf=df.select(df.EndDate).collect()[0]["EndDate"]
            Calendar_EndDate_conf = datetime.datetime.strptime(Calendar_EndDate_conf,"%Y-%m-%d").date()
            Calendar_EndDate_file=datetime.datetime.strptime(cdate,"%Y-%m-%d").date()
            Calendar_EndDate=min(Calendar_EndDate_conf,Calendar_EndDate_file)
            days = (Calendar_EndDate-UIStartDate).days
            
            data =[]
            for single_date in Kockpit.daterange(UIStartDate, Calendar_EndDate):
                data.append({'RollupMonth':single_date})
        
            schema = StructType([
                StructField("RollupMonth", DateType(),True)
            ])
            records=sqlCtx.createDataFrame(data,schema)
            records=records.select(last_day(records.RollupMonth).alias('RollupMonth')).distinct().sort('RollupMonth')
            records=records.withColumn("RollupMonth", \
                          when(records["RollupMonth"] ==    Kockpit.last_day_of_month(Calendar_EndDate_file), Calendar_EndDate_file).otherwise(records["RollupMonth"]))
            
            records.cache()
            print(records.count())
            ItemInv5=ItemInv3.join(records).where(records.RollupMonth>=ItemInv3.Monthend)
            ItemInv5 = ItemInv5.select("LinkItem","LocationCode","SBU_Code","BU_Code","RefNo","InDate","RollupMonth","StockQuantity","StockValue")
            ItemInv5 = RENAME(ItemInv5,{"LocationCode":"LinkLocation","RefNo":"RefILENo"})
            ItemInv6 = ItemInv5.groupby("LinkItem","LinkLocation","RefILENo","SBU_Code","BU_Code","InDate","RollupMonth")\
                                .agg({"StockQuantity":"sum","StockValue":"sum"})\
                                .drop("StockQuantity","StockValue")
            ItemInv6 = RENAME(ItemInv6,{"sum(StockQuantity)":"StockQuantity","sum(StockValue)":"StockValue","RollupMonth":"Monthend"})
            ItemInv7 = ItemInv6.filter((ItemInv6.StockQuantity!=0) | (ItemInv6.StockValue!=0))
            ItemInv7 = ItemInv7.withColumn("PostingDate",ItemInv7.Monthend)\
                        .withColumn("LinkDate",ItemInv7.Monthend)\
                        .withColumn("StockAge",datediff(ItemInv7.Monthend,ItemInv7.InDate))\
                        .withColumn("TransactionType",lit("StockAgeing")).drop('Monthend')
            ItemInv7=ItemInv7.na.fill({'StockAge':0})
            ItemInv7.cache()
            print(ItemInv7.count())
            Bucket = spark.read.format("delta").load(STAGE1_Configurator_Path+"/tblInventoryBucket")
            Maxoflt = Bucket.filter(Bucket['BucketName']=='<')
            MaxLimit = int(Maxoflt.select('UpperLimit').first()[0])
            Minofgt = Bucket.filter(Bucket['BucketName']=='>')
            MinLimit = int(Minofgt.select('LowerLimit').first()[0])
            ItemInv7 = ItemInv7.join(Bucket,ItemInv7.StockAge == Bucket.Nod,'left').drop('ID','UpperLimit','LowerLimit')
            ItemInv7=ItemInv7.withColumn('BucketName',when(ItemInv7.StockAge>=MinLimit,lit(str(MinLimit)+'+')).otherwise(ItemInv7.BucketName))\
                        .withColumn('Nod',when(ItemInv7.StockAge>=MinLimit,ItemInv7.StockAge).otherwise(ItemInv7.Nod))
            ItemInv7=ItemInv7.withColumn('BucketName',when(ItemInv7.StockAge<=(MaxLimit),lit("Adv. Issued")).otherwise(ItemInv7.BucketName))\
                        .withColumn('Nod',when(ItemInv7.StockAge<=(MaxLimit), ItemInv7.StockAge).otherwise(ItemInv7.Nod))
          
            ItemInv7 = ItemInv7.na.fill({'LinkLocation':'NA','LinkDate':'NA','LinkItem':'NA'})
            ItemInv7 = ItemInv7.withColumn("DBName",lit(DBName)).withColumn("EntityName",lit(EntityName))
            ItemInv7 = ItemInv7.withColumn("LinkLocationKey",concat_ws("|",ItemInv7.DBName,ItemInv7.EntityName,ItemInv7.LinkLocation))\
                            .withColumn("LinkDateKey",concat_ws("|",ItemInv7.DBName,ItemInv7.EntityName,ItemInv7.LinkDate))\
                            .withColumn("LinkItemKey",concat_ws("|",ItemInv7.DBName,ItemInv7.EntityName,ItemInv7.LinkItem))\
                            .withColumn("LinkSBUKey",concat_ws("|",ItemInv7.DBName,ItemInv7.EntityName,ItemInv7.SBU_Code))\
                            .withColumn("LinkBUKey",concat_ws("|",ItemInv7.DBName,ItemInv7.EntityName,ItemInv7.BU_Code))
            finalDF = ItemInv7.join(purchase_entries,ItemInv7["RefILENo"]==purchase_entries["VE_ILE_NO"],'left')
            finalDF.cache()
            print(finalDF.count())
            finalDF = RenameDuplicateColumns(finalDF).drop("Locationtype")
            finalDF.coalesce(1).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(STAGE2_PATH+"/"+"Inventory/StockAgeing")
           
            logger.endExecution()
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"
        
            log_dict = logger.getSuccessLoggedRecord("Inventory.StockAgeing", DBName, EntityName, finalDF.count(), len(finalDF.columns), IDEorBatch)
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
            os.system("spark-submit "+Kockpit_Path+"/Email.py 1 StockAgeing '"+CompanyName+"' "+DBEntity+" "+str(exc_traceback.tb_lineno)+" ")
            log_dict = logger.getErrorLoggedRecord('Inventory.StockAgeing', DBName, EntityName, str(ex), str(exc_traceback.tb_lineno), IDEorBatch)
            log_df = spark.createDataFrame(log_dict, logger.getSchema())
            log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)
print('StockAgeing completed: ' + str((dt.datetime.now()-st).total_seconds())) 

