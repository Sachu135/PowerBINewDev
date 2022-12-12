from pyspark.sql import SparkSession,SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import concat,concat_ws,year,when,month,to_date,lit,datediff,col
from pyspark.sql.types import *
import os,sys
from datetime import timedelta
from os.path import dirname, join, abspath
import datetime,time,traceback
import datetime as dt 
from builtins import str, len
from datetime import date
st = dt.datetime.now()
Kockpit_Path =abspath(join(join(dirname(__file__),'..','..','..','..','..')))
DB_path =abspath(join(join(dirname(__file__),'..','..','..','..')))
sys.path.insert(0,'../../')
sys.path.insert(0, DB_path)
from Configuration.AppConfig import * 
from Configuration.Constant import *
from Configuration.udf import *
from Configuration import udf as Kockpit
Kockpit_Path =abspath(join(join(dirname(__file__),'..','..','..','..','..')))
Filepath = os.path.dirname(os.path.abspath(__file__))
FilePathSplit = Filepath.split('/')
DBName = FilePathSplit[-5]
EntityName = FilePathSplit[-4]
DBEntity = DBName+EntityName
STAGE1_Configurator_Path=HDFS_PATH+DIR_PATH+"/" +DBName+"/" +EntityName+"/" +"Stage1/ConfiguratorData/"
STAGE1_PATH=HDFS_PATH+DIR_PATH+"/" +DBName+"/" +EntityName+"/" +"Stage1/ParquetData"
STAGE2_PATH=HDFS_PATH+DIR_PATH+"/" +DBName+"/" +EntityName+"/" +"Stage2/ParquetData"
sqlCtx,spark=getSparkConfig(SPARK_MASTER, "Stage2:PurchaseOrder")
def purchase_PurchaseOrder():
    cy = date.today().year
    cm = date.today().month
    cdate = datetime.datetime.now().strftime('%Y-%m-%d')
    for dbe in config["DbEntities"]:
        if dbe['ActiveInactive']=='true' and  dbe['Location']==DBEntity:
            CompanyName=dbe['Name']
            CompanyName=CompanyName.replace(" ","") 
            try:
                logger = Logger()
                phDF =spark.read.format("delta").load(STAGE1_PATH+"/Purchase Header")
                plDF = spark.read.format("delta").load(STAGE1_PATH+"/Purchase Line")
                DSE=spark.read.format("delta").load(STAGE2_PATH+"/"+"Masters/DSE")
                PDDBucket =spark.read.format("delta").load(STAGE1_Configurator_Path+"/tblPDDBucket")
                SH = spark.read.format("delta").load(STAGE1_PATH+"/Sales Header").select('No_','Bill-toCustomerNo_')\
                            .withColumnRenamed('No_','SalesOrderNo').withColumnRenamed('Bill-toCustomerNo_','CustomerCode')
                SL = spark.read.format("delta").load(STAGE1_PATH+"/Sales Line").select('DocumentNo_','LineNo_','Pur_OrderNo_','POLineNo_')\
                            .withColumnRenamed('DocumentNo_','SalesOrderNo').withColumnRenamed('LineNo_','SalesOrderLineNo')
                SO = SL.join(SH,'SalesOrderNo', 'left')
                SO = SO.filter(SO['Pur_OrderNo_']!='')
                SO = SO.withColumnRenamed('Pur_OrderNo_','No_').withColumnRenamed('POLineNo_','LineNo_')
                phDF = phDF.withColumn("LinkDate",to_date(phDF.PostingDate))\
                            .withColumn("LinkVendor",when(phDF["Pay-toVendorNo_"]=='',"NA").otherwise(phDF["Pay-toVendorNo_"]))\
                            .withColumn("RequestedReceiptDate",when((year(phDF.RequestedReceiptDate)<1900)|(phDF.RequestedReceiptDate.isNull()),to_date(phDF.PostingDate)).otherwise(to_date(phDF.RequestedReceiptDate)))\
                            .withColumn("PromisedReceiptDate",when((year(phDF.PromisedReceiptDate)<1900)|(phDF.PromisedReceiptDate.isNull()),to_date(phDF.PostingDate)).otherwise(to_date(phDF.PromisedReceiptDate)))
                plDF = plDF.withColumnRenamed('DocumentType','PLDocumentType')\
                            .withColumnRenamed('PostingDate','PLPostingDate')\
                            .withColumnRenamed('No_','ItemNo_')
                PO = plDF.join(phDF, plDF['DocumentNo_']==phDF['No_'], 'left')
                PO = PO.filter(PO['DocumentType']==1).filter(year(PO['PostingDate'])!=1753)
                PO = Kockpit.RenameDuplicateColumns(PO)
                PO = PO.withColumn("NOD_OrderDate",datediff(lit(datetime.datetime.today()),PO['OrderDate']))
                PO = PO.join(SO,['No_','LineNo_'],'left')
                PO =  PO.withColumn("LineAmount",when((PO.LineAmount/PO.CurrencyFactor).isNull(),PO.LineAmount).otherwise(PO.LineAmount/PO.CurrencyFactor))\
                        .withColumn("Transaction_Type",lit("PurchaseOrder"))      
                Maxoflt = PDDBucket.filter(PDDBucket['BucketName']=='<')
                MaxLimit = int(Maxoflt.select('MaxLimit').first()[0])
                Minofgt = PDDBucket.filter(PDDBucket['BucketName']=='>')
                MinLimit = int(Minofgt.select('MinLimit').first()[0])
                PO = PO.join(PDDBucket,PO.NOD_OrderDate == PDDBucket.Nod,'left').drop('ID','UpperLimit','LowerLimit')
                PO=PO.withColumn('BucketName',when(PO.NOD_OrderDate>=MinLimit,lit(str(MinLimit)+'+')).otherwise(PO.BucketName))\
                            .withColumn('Nod',when(PO.NOD_OrderDate>=MinLimit,PO.NOD_OrderDate).otherwise(PO.Nod))
                PO=PO.withColumn('BucketName',when(PO.NOD_OrderDate<=(MaxLimit),lit("Not Due")).otherwise(PO.BucketName))\
                            .withColumn('Nod',when(PO.NOD_OrderDate<=(MaxLimit), PO.NOD_OrderDate).otherwise(PO.Nod))
                finalDF = PO.join(DSE,"DimensionSetID",'left')
                finalDF = RenameDuplicateColumns(finalDF).drop("locationtype")
                finalDF.write.option("maxRecordsPerFile", 10000).format("delta").mode("overwrite").option("overwriteSchema", "true").save(STAGE2_PATH+"/"+"Purchase/PurchaseOrder")
                logger.endExecution()
                
                try:
                    IDEorBatch = sys.argv[1]
                except Exception as e :
                    IDEorBatch = "IDLE"
            
                log_dict = logger.getSuccessLoggedRecord("Purchase.PurchaseOrder", DBName, EntityName, finalDF.count(), len(finalDF.columns), IDEorBatch)
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
                os.system("spark-submit "+Kockpit_Path+"/Email.py 1  PurchaseOrder '"+CompanyName+"' "+DBEntity+" "+str(exc_traceback.tb_lineno)+" ")
                log_dict = logger.getErrorLoggedRecord('Purchase.PurchaseOrder', DBName, EntityName, str(ex), str(exc_traceback.tb_lineno), IDEorBatch)
                log_df = spark.createDataFrame(log_dict, logger.getSchema())
                log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)
    print('purchase_PurchaseOrder completed: ' + str((dt.datetime.now()-st).total_seconds()))
if __name__ == "__main__":
    purchase_PurchaseOrder()     
    