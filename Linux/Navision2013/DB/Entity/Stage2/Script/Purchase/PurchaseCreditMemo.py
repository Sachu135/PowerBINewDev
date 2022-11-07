from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col,concat,concat_ws,year,when,month,to_date,lit,quarter,sum,count
from datetime import date
import datetime as dt
import os,sys
from os.path import dirname, join, abspath
st = dt.datetime.now()
Kockpit_Path =abspath(join(join(dirname(__file__),'..','..','..','..','..')))
DB1_path =abspath(join(join(dirname(__file__),'..','..','..','..')))
sys.path.insert(0,'../../')
sys.path.insert(0, DB1_path)
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
STAGE1_Configurator_Path=Kockpit_Path+"/" +DBName+"/" +EntityName+"/" +"Stage1/ConfiguratorData/"
STAGE1_PATH=Kockpit_Path+"/" +DBName+"/" +EntityName+"/" +"Stage1/ParquetData"
STAGE2_PATH=Kockpit_Path+"/" +DBName+"/" +EntityName+"/" +"Stage2/ParquetData"
conf = SparkConf().setMaster(SPARK_MASTER).setAppName("PurchaseCreditMemo")\
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
        .set("spark.kryoserializer.buffer.max","512m")\
        .set("spark.cores.max","24")\
        .set("spark.executor.memory","10g")\
        .set("spark.driver.memory","24g")\
        .set("spark.driver.maxResultSize","20g")\
        .set("spark.memory.offHeap.enabled",'true')\
        .set("spark.memory.offHeap.size","100g")\
        .set('spark.scheduler.mode', 'FAIR')\
        .set("spark.sql.broadcastTimeout", "36000")\
        .set("spark.network.timeout", 10000000)\
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
for dbe in config["DbEntities"]:
    if dbe['ActiveInactive']=='true' and  dbe['Location']==DBEntity:
        CompanyName=dbe['Name']
        CompanyName=CompanyName.replace(" ","")
        try:
            
            logger = Logger()
            pcml = spark.read.format("delta").load(STAGE1_PATH+"/Purch_ Cr_ Memo Line")
            pcmh = spark.read.format("delta").load(STAGE1_PATH+"/Purch_ Cr_ Memo Hdr_")
            gps =spark.read.format("delta").load(STAGE1_PATH+"/General Posting Setup")
            pld = spark.read.format("delta").load(STAGE1_PATH+"/Posted Str Order Line Details")
            VE = spark.read.format("delta").load(STAGE1_PATH+"/Value Entry")
            dse=spark.read.format("delta").load(STAGE2_PATH+"/"+"Masters/DSE")
            PurchaseInvoice=spark.read.format("delta").load(STAGE2_PATH+"/"+"Purchase/PurchaseInvoice")    
            GLRange=spark.read.format("delta").load(STAGE1_Configurator_Path+"/tblGLAccountMapping")
            pcml = pcml.filter((year(col("PostingDate"))!='1753')&(col("Quantity")!=0)&(col("Type")!=4))
            Line = pcml\
                    .withColumn("LinkItem",when(pcml["Type"]==2,when(pcml["No_"]=='',lit("NA")).otherwise(pcml["No_"])))\
                    .withColumn("No",when(col("Type")==1,col("No_").cast('int')))\
                    .withColumn("GL_Link",concat_ws("|",lit(entityLocation),pcml.Gen_Bus_PostingGroup,pcml.Gen_Prod_PostingGroup))\
                    .withColumn("LinkValueEntry",concat_ws("|",pcml.DocumentNo_.cast('string'),pcml.LineNo_.cast('string'),to_date(pcml.PostingDate).cast('string')))\
                    .withColumn("Link_GD",concat_ws("-",pcml.DocumentNo_,pcml.LineNo_))\
                    .withColumn("LinkLocationCode",when(col("LocationCode")=='',"NA").otherwise(col("LocationCode")))\
                    .withColumn("InvoicedQuantity",when(col("Type")==2,col("Quantity")*(-1)))\
                    .withColumn("ServiceTaxAmount",(pcml.ServiceTaxeCessAmount+pcml.ServiceTaxSHECessAmount+pcml.ServiceTaxAmount)*(-1))\
                    .withColumnRenamed("DimensionSetID","DimSetID").withColumnRenamed("DocumentNo_","Document_No").withColumnRenamed("LineNo_","LineNo")\
                    .withColumnRenamed("Description","LineDescription").withColumnRenamed("Quantity","Trn_Quantity")\
                    .withColumn("CrMemoAmount",col("Amount")*(-1))\
                    .withColumn("TaxAmount",col("TaxAmount")*(-1))\
                    .withColumn("ExciseAmount",col("ExciseAmount")*(-1))\
                    .withColumnRenamed("Inv_DiscountAmount","InvDiscountAmount")\
                    .withColumnRenamed("Buy-from Vendor No_","BuyfromVendorNumber")
                    
            Line = Line.withColumn('PurchaseTaxAmount',Line['TaxAmount'])
            Line = Line.drop('PostingDate')
            Header = pcmh\
                    .withColumn("LinkVendor",when(pcmh['Pay-toVendorNo_']=='','NA').otherwise(pcmh["Pay-toVendorNo_"])).drop("Pay-toVendorNo_","DBName","EntityName")\
                    .withColumn("PaytoName",pcmh['Pay-toName']+" "+pcmh['Pay-toName2']).drop('Pay-toName','Pay-toName2')\
                    .withColumn("PaytoAddress",pcmh['Pay-toAddress']+" "+pcmh['Pay-toAddress2']).drop('Pay-toAddress','Pay-toAddress2')\
                    .withColumn("ShiptoName",pcmh['Ship-toName']+" "+pcmh['Ship-toName2']).drop('Ship-toName','Ship-toName2')\
                    .withColumn("ShiptoAddress",pcmh['Ship-toAddress']+" "+pcmh['Ship-toAddress2']).drop('Ship-toAddress','Ship-toAddress2')\
                    .withColumn("LinkDate",to_date(pcmh.PostingDate))\
                    .withColumn("LinkPurchaseRep",when(pcmh.PurchaserCode=='','NA').otherwise(pcmh.PurchaserCode)).drop("PurchaserCode")\
                    .withColumn("BuyfromVendorName",pcmh["Buy-fromVendorName"]+" "+pcmh["Buy-fromVendorName2"]).drop("Buy-fromVendorName","Buy-fromVendorName2")\
                    .withColumn("Years",when(month(pcmh.PostingDate)>=MnSt,year(pcmh.PostingDate)).otherwise(year(pcmh.PostingDate)-1))\
                    .withColumn("Quarters",when(month(pcmh.PostingDate)>=MnSt,concat(lit('Q'),(quarter(pcmh.PostingDate)-1))).otherwise(lit('Q4')))\
                    .withColumn("MonthNum", month(pcmh.PostingDate))\
                    .withColumnRenamed("No_","Purchase_No").withColumnRenamed("Pay-toCity","PaytoCity").withColumnRenamed("Ship-toCode","ShiptoCode")\
                    .withColumnRenamed("Ship-toCity","ShiptoCity").withColumnRenamed("OnHold","HoldStatus")\
                    .withColumnRenamed("LocationCode","HeaderLocationCode")\
                    .withColumnRenamed("Buy-fromCity","BuyfromCity").withColumnRenamed("Pay-toPostCode","PaytoPostCode")\
                    .withColumnRenamed("Ship-toPostCode","ShiptoPostCode")
           
            Monthsdf =Kockpit.MONTHSDF(sqlCtx)
            mcond = [Header.MonthNum==Monthsdf.MonthNum1]
            Header = Kockpit.LJOIN(Header,Monthsdf,mcond)
            Header = Header.drop('MonthNum','MonthNum1')
            GL_Master = gps.withColumn("GL_LinkDrop",concat_ws('|',lit(entityLocation),gps.Gen_Bus_PostingGroup,gps.Gen_Prod_PostingGroup)).drop('Gen_Bus_PostingGroup','Gen_Prod_PostingGroup')\
                                    .withColumn("GLAccount",when(gps.Purch_Account=='',0).otherwise(gps.Purch_Account)).drop('Purch_Account')
                
            GLRange = GLRange.filter(GLRange['DBName'] == DBName ).filter(GLRange['EntityName'] == EntityName ).filter(GLRange['GLRangeCategory']== 'Purchase Trading')
            GLRange = Kockpit.RENAME(GLRange,{'FromGLCode':'FromGL','ToGLCode':'ToGL','GLRangeCategory':'GLCategory'})
            GLRange = GLRange.select("GLCategory","FromGL","ToGL")
            cond = [Line.Document_No == Header.Purchase_No]
            Purchase = Kockpit.LJOIN(Line,Header,cond)
            cond1 = [Purchase.GL_Link == GL_Master.GL_LinkDrop]
            Purchase = Kockpit.LJOIN(Purchase,GL_Master,cond1)
            Purchase = Purchase.drop("GL_LinkDrop","GL_Link")
            
            Range='1=1'
            Range1='1=1'
            NoOfRows=GLRange.count()
            for i in range(0,NoOfRows):
                if i==0:
                    Range = (Purchase.GLAccount>=GLRange.select('FromGL').collect()[0]["FromGL"])\
                        & (Purchase.GLAccount<=GLRange.select('ToGL').collect()[0]["ToGL"])
            
                    Range1 = (Purchase.No>=GLRange.select('FromGL').collect()[0]["FromGL"])\
                        & (Purchase.No<=GLRange.select('ToGL').collect()[0]["ToGL"])
                else:
                    Range = (Range) | (Purchase.GLAccount>=GLRange.select('FromGL').collect()[i]["FromGL"])\
                                        & (Purchase.GLAccount<=GLRange.select('ToGL').collect()[i]["ToGL"])
            
                    Range1 = (Range1) | (Purchase.No>=GLRange.select('FromGL').collect()[i]["FromGL"])\
                                        & (Purchase.No<=GLRange.select('ToGL').collect()[i]["ToGL"])
            Purchase = Purchase.filter(((Purchase.Type!=2) | ((Purchase.Type==2) & (Range))) & ((Purchase.Type!=1) | ((Purchase.Type==1) & (Range1))))
            Purchase = Purchase.withColumn("PurchaseAccount",when(Purchase.Type==2,Purchase.GLAccount).otherwise(when(Purchase.Type==1,Purchase.No).otherwise(lit(1))))
            
            pld = pld.filter((pld.Type == 2) & (pld.DocumentType==2) & (pld.Tax_ChargeType==0))
            LineDetails = pld.withColumn("Link_GDDrop",concat_ws('-',pld.InvoiceNo_,pld.LineNo_)).drop("InvoiceNo_","LineNo_")\
                                                    .withColumnRenamed("AccountNo_","AccountNo")
            Range2 = LineDetails['AccountNo']!=0
            NoOfRows = GLRange.count()
            for j in range(0,NoOfRows):
                if i==0:
                    FromGL = "%s"%GLRange.select(GLRange.FromGL).collect()[0]['FromGL']
                    ToGL = "%s"%GLRange.select(GLRange.ToGL).collect()[0]['ToGL']
                    Range2 = (LineDetails['AccountNo']>=FromGL) & (LineDetails['AccountNo']<=ToGL)
                else:
                    FromGL = "%s"%GLRange.select(GLRange.FromGL).collect()[i]['FromGL']
                    ToGL = "%s"%GLRange.select(GLRange.ToGL).collect()[i]['ToGL']
                    Range2 = (LineDetails['AccountNo']>=FromGL) & (LineDetails['AccountNo']<=ToGL)
            LineDetails = LineDetails[Range2].drop("DBName","EntityName")
            LineDetails=LineDetails.groupby('Link_GDDrop').sum('Amount').withColumnRenamed('sum(Amount)','ChargesfromVendor')
            
            cond2 = [Purchase.Link_GD == LineDetails.Link_GDDrop]
            Purchase = Kockpit.LJOIN(Purchase,LineDetails,cond2)
            Purchase = Purchase.drop("Link_GDDrop")
            Purchase = Purchase.withColumn('TransactionType',lit('CrMemo'))
            Purchase = Purchase\
                            .withColumn('GLAccountNo',when(Purchase.GLAccount.isNull(), Purchase.No).otherwise(Purchase.GLAccount))\
                            .withColumn('PayableAmount',when((Purchase.CurrencyFactor==0) | (Purchase.CurrencyFactor.isNull()),when(Purchase.ChargesfromVendor.isNull(),Purchase.CrMemoAmount)\
                            .otherwise(Purchase.CrMemoAmount+Purchase.ChargesfromVendor)).otherwise(when(Purchase.ChargesfromVendor.isNull(),(Purchase.CrMemoAmount/Purchase.CurrencyFactor))\
                            .otherwise(((Purchase.CrMemoAmount+Purchase.ChargesfromVendor)/(Purchase.CurrencyFactor)))))
                    
            Purchase = Purchase.withColumn('LinkLocation',when(Purchase.LinkLocationCode.isNull(),Purchase.HeaderLocationCode).otherwise(Purchase.LinkLocationCode))
            Purchase = Purchase.drop('PurchaseAccount')
            Purchase = Purchase.withColumn('LinkDate',to_date(Purchase['PostingDate']))
            FieldName = "CostAmount(Expected)"
           
            ValueEntry = VE.withColumn("LinkDate",to_date(VE.PostingDate))\
                        .withColumn("LinkValueEntry1",concat_ws("|",VE.DocumentNo_,VE.DocumentLineNo_,to_date(VE.PostingDate).cast('string')))\
            
            ValueEntry = Kockpit.RENAME(ValueEntry,{"DimensionSetID":"DimSetID","ItemNo_":"LinkItem","DocumentNo_":"DocumentNo","ItemLedgerEntryNo_":"LinkILENo"}).drop('DBName','EntityName','YearMonth')
            
            if FieldName=='CostAmount(Expected)':
                ValueEntry = ValueEntry.withColumn("CostAmount",when(ValueEntry.CostAmountActual==0,ValueEntry.CostAmountExpected*(-1)).otherwise(ValueEntry.CostAmountActual*(-1)))
            else:
                ValueEntry = ValueEntry.withColumn("CostAmount",ValueEntry[FieldName]*(-1))
        
            ValueEntry1 = ValueEntry.select('LinkValueEntry1','CostAmount').groupby('LinkValueEntry1')\
                        .agg(sum("CostAmount").alias("CostAmount"))
            
            JoinCOGS = [Purchase.LinkValueEntry == ValueEntry1.LinkValueEntry1]
            Purchase =  Kockpit.LJOIN(Purchase,ValueEntry1,JoinCOGS)
            Purchase = Purchase.drop("CrMemoAmount","LinkValueEntry1")
        
            Documents = Purchase.select('Document_No').distinct()
            Documents = Documents.withColumn('SysDocFlag',lit(1)).withColumnRenamed('Document_No','Document_NoDrop')
            
            DocumentsCrMemo = PurchaseInvoice.select("Document_No").distinct()
            DocumentsCrMemo = DocumentsCrMemo.withColumnRenamed("Document_No","Document_NoDrop").withColumn("SysDocFlag",lit(2))
            Documents=Documents.unionAll(DocumentsCrMemo)
            
            JoinDocs = [ValueEntry.DocumentNo == Documents.Document_NoDrop]
            ValueEntry = Kockpit.LJOIN(ValueEntry,Documents,JoinDocs)
            ValueEntry = ValueEntry.drop("Document_NoDrop")
            SysEntries = Purchase.select('LinkValueEntry').distinct()
            SysEntries = SysEntries.withColumn('SysValueEntryFlag',lit(1))
            JoinSysEntry = [ValueEntry.LinkValueEntry1 == SysEntries.LinkValueEntry]
            ValueEntry = Kockpit.LJOIN(ValueEntry,SysEntries,JoinSysEntry)
            ValueEntry = ValueEntry.drop("LinkValueEntry").withColumnRenamed("LinkValueEntry1","LinkValueEntry")
            ValueEntry = ValueEntry.filter(((ValueEntry.SysDocFlag==1)&(ValueEntry.SysValueEntryFlag.isNull()))|((ValueEntry.SysDocFlag.isNull())&(ValueEntry.ItemLedgerEntryType==1)))
            ValueEntry = ValueEntry.withColumnRenamed('DocumentNo','Document_No').withColumnRenamed('DocumentLineNo_','LineNo')\
                                .withColumnRenamed('EntryNo_','Link_GD').withColumn('Trn_Quantity',ValueEntry['InvoicedQuantity'])\
                                .withColumn('RevenueType',lit('OTHER')).withColumn('TransactionType',lit('RevaluationandRemainingEntries'))\
                                .withColumnRenamed('LocationCode','LinkLocation')
                            
            dse = Kockpit.RENAME(dse,{"DimensionSetID":"DimSetID1"})
            scond = [Purchase.DimSetID==dse.DimSetID1]
            Purchase = Kockpit.LJOIN(Purchase,dse,scond)
            Purchase = Purchase.drop('DimSetID1')
            vcond = [ValueEntry.DimSetID==dse.DimSetID1]
            ValueEntry = Kockpit.LJOIN(ValueEntry,dse,vcond)
            ValueEntry = ValueEntry.withColumn("PostingDate",col("PostingDate").cast('string'))\
                    .withColumn("LinkDate",col("LinkDate").cast('string')).drop('DimSetID1')
            Purchase = Kockpit.RenameDuplicateColumns(Purchase).drop("Locationtype")
            Purchase = Purchase.withColumn("PostingDate",col("PostingDate").cast('string'))\
                        .withColumn("ExpectedReceiptDate",col("ExpectedReceiptDate").cast('string'))\
                        .drop('LocationCode','Amount','ServiceTaxSHECessAmount','Sell-toCustomerName2','GLCode','Link_GDDrop')
            
            Purchase=CONCATENATE(Purchase,ValueEntry,spark)
            Purchase = Purchase.withColumnRenamed('LinkPurchaseRep','LinkPurchaser')
            Purchase.cache()
            print(Purchase.count())
            Purchase = Purchase.drop('BudColumn')
            Purchase=Purchase.withColumn("DBName",concat(lit(DBName))).withColumn("EntityName",concat(lit(EntityName)))
                
            Purchase = Purchase.withColumn('DBName',lit(DBName)).withColumn('EntityName',lit(EntityName))
            Purchase = Purchase.withColumn('LinkVendorKey',concat_ws('|',Purchase.DBName,Purchase.EntityName,Purchase.LinkVendor))\
                                .withColumn('LinkDateKey',concat_ws('|',Purchase.DBName,Purchase.EntityName,Purchase.LinkDate))\
                                .withColumn('LinkPurchaserKey',concat_ws('|',Purchase.DBName,Purchase.EntityName,Purchase.LinkPurchaser))\
                                .withColumn('LinkItemKey',concat_ws('|',Purchase.DBName,Purchase.EntityName,Purchase.LinkItem))\
                                .withColumn('LinkLocationKey',concat_ws('|',Purchase.DBName,Purchase.EntityName,Purchase.LinkLocation))
            PurchaseInv=CONCATENATE(Purchase,PurchaseInvoice,spark)
            PurchaseInv = PurchaseInv.na.fill({"PayableAmount":0})
            PurchaseInv.cache()
            print(PurchaseInv.count())
            PurchaseInv.coalesce(1).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(STAGE2_PATH+"/"+"Purchase/Purchase") 
            logger.endExecution()
         
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"
            log_dict = logger.getSuccessLoggedRecord("Purchase.PurchaseCreditMemo", DBName, EntityName, PurchaseInv.count(), len(PurchaseInv.columns), IDEorBatch)
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
            os.system("spark-submit "+Kockpit_Path+"/Email.py 1 PurchaseCreditMemo '"+CompanyName+"' "+DBEntity+" "+str(exc_traceback.tb_lineno)+" ")
                    
            log_dict = logger.getErrorLoggedRecord('Purchase.PurchaseCreditMemo', DBName, EntityName, str(ex), str(exc_traceback.tb_lineno), IDEorBatch)
            log_df = spark.createDataFrame(log_dict, logger.getSchema())
            log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)
print('purchase_PurchaseCreditMemo completed: ' + str((dt.datetime.now()-st).total_seconds()))

