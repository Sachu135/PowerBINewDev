from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import io,re,os,datetime
from datetime import timedelta, date
from pyspark.sql.functions import col,max as max_,min as min_,concat,year,when,month,to_date,lit,sum,last_day,datediff
import sys
from pyspark.sql.types import *
from builtins import str
import traceback
from os.path import dirname, join, abspath
import datetime as dt
st = dt.datetime.now()
Kockpit_Path =abspath(join(join(dirname(__file__),'..','..','..','..','..')))
DB_path =abspath(join(join(dirname(__file__),'..','..','..','..')))
sys.path.insert(0,'../../')
sys.path.insert(0, DB_path)
from Configuration.AppConfig import * 
from Configuration.Constant import *
from Configuration.udf import *
from Configuration import udf as Kockpit
Filepath = os.path.dirname(os.path.abspath(__file__))
FilePathSplit = Filepath.split('\\')
DBName = FilePathSplit[-5]
EntityName = FilePathSplit[-4]
DBEntity = DBName+EntityName
entityLocation = DBName+EntityName
STAGE1_Configurator_Path=Kockpit_Path+"/" +DBName+"/" +EntityName+"/" +"Stage1/ConfiguratorData/"
STAGE1_PATH=Kockpit_Path+"/" +DBName+"/" +EntityName+"/" +"Stage1/ParquetData"
STAGE2_PATH=Kockpit_Path+"/" +DBName+"/" +EntityName+"/" +"Stage2/ParquetData"
conf = SparkConf().setMaster("local[16]").setAppName("Payables_Snapshot").\
                    set("spark.sql.shuffle.partitions",16).\
                    set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").\
                    set("spark.local.dir", "/tmp/spark-temp").\
                    set("spark.driver.memory","30g").\
                    set("spark.executor.memory","30g").\
                    set("spark.driver.cores",16).\
                    set("spark.driver.maxResultSize","0").\
                    set("spark.sql.debug.maxToStringFields", "1000").\
                    set("spark.executor.instances", "20").\
                    set('spark.scheduler.mode', 'FAIR').\
                    set("spark.sql.broadcastTimeout", "36000").\
                    set("spark.network.timeout", 10000000).\
                    set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY").\
                    set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "LEGACY").\
                    set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED").\
                    set("spark.sql.legacy.timeParserPolicy","LEGACY").\
                    set("spark.sql.legacy.parquet.int96RebaseModeInWrite","LEGACY").\
                    set("spark.sql.legacy.parquet.int96RebaseModeInWrite","CORRECTED")
sc = SparkContext(conf = conf)
sqlCtx = SQLContext(sc)
spark = sqlCtx.sparkSession
fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
cdate = datetime.datetime.now().strftime('%d-%m-%Y')
for dbe in config["DbEntities"]:
    if dbe['ActiveInactive']=='true' and  dbe['Location']==DBEntity:
        CompanyName=dbe['Name']
        CompanyName=CompanyName.replace(" ","")
        try:
            logger = Logger()
            Company =spark.read.format("parquet").load(STAGE1_Configurator_Path+"/tblCompanyName")
            Company = Company.filter(col('DBName')==DBName).filter(col('NewCompanyName') == EntityName)
            df = Company.select("StartDate","EndDate")
            Calendar_StartDate = df.select(df.StartDate).collect()[0]["StartDate"]
            Calendar_StartDate = datetime.datetime.strptime(Calendar_StartDate,"%m/%d/%Y").date()
           
            if datetime.date.today().month>int(MnSt)-1:
                    UIStartYr=datetime.date.today().year-int(yr)+1
            else:
                    UIStartYr=datetime.date.today().year-int(yr)
            UIStartDate=datetime.date(UIStartYr,int(MnSt),1)
            UIStartDate=max(Calendar_StartDate,UIStartDate)
            VLE = spark.read.format("parquet").load(STAGE1_PATH+"/Vendor Ledger Entry")
            DVLE = spark.read.format("parquet").load(STAGE1_PATH+"/Detailed Vendor Ledg_ Entry")
            DVLE = DVLE.withColumnRenamed('DocumentType','DVLE_Document_Type')
            VPG = spark.read.format("parquet").load(STAGE1_PATH+"/Vendor Posting Group")
            PIH = spark.read.format("parquet").load(STAGE1_PATH+"/Purch_ Inv_ Header")
            pih = PIH.select('No_','PaymentTermsCode').withColumnRenamed('No_','PIH_No')
            DSE=spark.read.format("parquet").load(STAGE2_PATH+"/"+"Masters/DSE").drop("DBName","EntityName")
            vle = VLE.withColumn("LinkVendor",when(col("VendorNo_")=='',"NA").otherwise(col("VendorNo_")))\
                     .withColumn("LinkPurchaser",when(col("PurchaserCode")=='',"NA").otherwise(col("PurchaserCode")))\
                     .withColumn("Due_Date",to_date(col("DueDate")))
            vle = vle.withColumnRenamed("DimensionSetID","DimSetID").withColumnRenamed("EntryNo_","VLE_No")\
                     .withColumnRenamed("DocumentNo_","VLE_Document_No").withColumnRenamed("Description","VLE_Description")\
                 .withColumnRenamed("VendorPostingGroup","Vendor_Posting_Group").withColumnRenamed("ExternalDocumentNo_","ExternalDocumentNo")\
                 .withColumnRenamed("PostingDate","VLE_Posting_Date").withColumnRenamed("PostingDate","LinkDate")
            
            vle = vle.join(pih,vle["VLE_Document_No"]==pih["PIH_No"],'left')
            current_month = datetime.datetime.strptime(cdate,"%d-%m-%Y")
            current_month = str(current_month.year)+str(current_month.month)
            dvle = DVLE.filter(year(col("PostingDate"))!='1753')
            dvle = dvle.withColumn('AmountLCY',dvle['AmountLCY'].cast('decimal(20,4)'))
            dvle = dvle.withColumn("Original_Amount",when(col("EntryType")==1,col("AmountLCY")*(-1)).otherwise(col("AmountLCY")*(-1)))\
                       .withColumn("DVLE_Posting_Date",to_date(col("PostingDate")))\
                       .withColumn("link_month",concat(year(dvle.PostingDate),month(dvle.PostingDate)))\
                       .withColumn("Transaction_Type",lit("VLE Entry"))\
                       .withColumn("Remaining_Amount",col("AmountLCY")*(-1)).drop('DBName','EntityName')
            dvle = dvle.withColumnRenamed("VendorLedgerEntryNo_","DVVLE_No").withColumnRenamed("DocumentNo_","DVLE_Document_No")
            
            dvle = dvle.withColumn("DVLE_Monthend_Posting_Date",when(dvle.link_month==current_month, cdate).otherwise(last_day(dvle.PostingDate)))
            dvle = dvle.drop('link_month').drop('DocumentDate')
            cond = [vle.VLE_No==dvle.DVVLE_No]
            df = RJOIN(vle,dvle,cond)
            df1 = VPG.withColumnRenamed("Code","Vendor_Posting_Group").withColumnRenamed("PayablesAccount","GLAccount")
            cond = [df.Vendor_Posting_Group == df1.Vendor_Posting_Group]
            df2 = LJOIN(df,df1,cond)
            GLRange=spark.read.format("parquet").load(STAGE1_Configurator_Path+"/tblGLAccountMapping")
            GLRange = GLRange.filter(GLRange['DBName'] == DBName ).filter(col('EntityName') == EntityName).filter(GLRange['GLRangeCategory']== 'Vendor')
            GLRange = GLRange.withColumnRenamed("FromGLCode", "FromGL")
            GLRange = GLRange.withColumnRenamed("ToGLCode", "ToGL")
            GLRange = GLRange.withColumnRenamed("GLRangeCategory","GLCategory")
            GLRange = GLRange.select("GLCategory","FromGL","ToGL")
            
            Range='1=1'
            NoOfRows=GLRange.count()
            for i in range(0,NoOfRows):
                if i==0:
                        Range="(GLAccount>=%s"%GLRange.select(GLRange.FromGL).collect()[0]["FromGL"]+\
                        " AND GLAccount<=%s"%GLRange.select(GLRange.ToGL).collect()[0]["ToGL"]+')'
                else:
                        Range=Range+" OR (GLAccount>=%s"%GLRange.select(GLRange.FromGL).collect()[i]["FromGL"]+\
                        " AND GLAccount<=%s"%GLRange.select(GLRange.ToGL).collect()[i]["ToGL"]+')'
            df2.createOrReplaceTempView('temptable')
            df2=sqlCtx.sql("SELECT * FROM temptable Where ("+Range+")")

            df4 = df2.groupBy('VLE_No').agg({'Remaining_Amount':'sum'}).withColumnRenamed('sum(Remaining_Amount)','Remaining_Amount').filter('Remaining_Amount!=0')\
            .select('VLE_No','Remaining_Amount').withColumnRenamed('VLE_No','VLENo')
            df_DVLE_Temp1=df.select(df2.VLE_No,df2.VLE_Document_No,df2.DVLE_Monthend_Posting_Date)
            cond1 = [df_DVLE_Temp1.VLE_No==df4.VLENo]
            Df_min_max_date=LJOIN(df_DVLE_Temp1,df4,cond1)
            Df_min_max_date=Df_min_max_date.select(df_DVLE_Temp1.VLE_No,df_DVLE_Temp1.VLE_Document_No,df_DVLE_Temp1.DVLE_Monthend_Posting_Date,df4.Remaining_Amount).distinct()
            Df_min_max_date=Df_min_max_date.select('VLE_No','VLE_Document_No','DVLE_Monthend_Posting_Date','Remaining_Amount')
            sqldf = Df_min_max_date.withColumn('Max_Monthend',when(Df_min_max_date['Remaining_Amount']!=0, datetime.datetime.now().date().replace(month=12, day=31))\
                        .otherwise(Df_min_max_date['DVLE_Monthend_Posting_Date']))
            sqldf = sqldf.groupby('VLE_No','VLE_Document_No','Remaining_Amount').agg({'DVLE_Monthend_Posting_Date':'min','Max_Monthend':'max'})\
                                    .withColumnRenamed('VLE_No','TempVLE_No').withColumnRenamed('min(DVLE_Monthend_Posting_Date)','Min_Monthend')\
                                    .withColumnRenamed('max(Max_Monthend)','Max_Monthend')
            df = Company.select("StartDate","EndDate")
            Calendar_StartDate = df.select(df.StartDate).collect()[0]["StartDate"]
            Calendar_StartDate = datetime.datetime.strptime(Calendar_StartDate,"%m/%d/%Y").date()
            if datetime.date.today().month>int(MnSt)-1:
                UIStartYr=datetime.date.today().year-int(yr)+1
            else:
                UIStartYr=datetime.date.today().year-int(yr)
            UIStartDate=datetime.date(UIStartYr,int(MnSt),1)
            
            Calendar_EndDate_conf=df.select(df.EndDate).collect()[0]["EndDate"]
            Calendar_EndDate_conf = datetime.datetime.strptime(Calendar_EndDate_conf,"%m/%d/%Y").date()
            Calendar_EndDate_file=datetime.datetime.strptime(cdate,"%d-%m-%Y").date()
            Calendar_EndDate=min(Calendar_EndDate_conf,Calendar_EndDate_file)
            def last_day_of_month(date):
                    if date.month == 12:
                            return date.replace(day=31)
                    return date.replace(month=date.month+1, day=1) - datetime.timedelta(days=1)
            
            def daterange(start_date, end_date):
                    for n in range(int ((end_date - start_date).days)):
                            yield start_date + timedelta(n)
            
            data =[]
            for single_date in daterange(UIStartDate, Calendar_EndDate+timedelta(days=1)):
                data.append({'Link_date':single_date})
            
            schema = StructType([
                StructField("Link_date", DateType(),True)
            ])
            records=spark.createDataFrame(data,schema)
            records=records.select(last_day(records.Link_date).alias('Link_date')).distinct().sort('Link_date')
            records=records.withColumn("Link_date", \
                          when(records["Link_date"] == last_day_of_month(Calendar_EndDate_file), Calendar_EndDate_file).otherwise(records["Link_date"]))
            
            sqldf = JOIN(sqldf,records)
            sqldf = sqldf.select('TempVLE_No','VLE_Document_No','Min_MonthEnd','Max_MonthEnd','Link_date')
            sqldf=sqldf.filter(sqldf['Link_date']<= sqldf['Max_MonthEnd']).filter(sqldf['Link_date']>= sqldf['Min_MonthEnd']).select('TempVLE_No','VLE_Document_No','Link_date').withColumnRenamed('Link_date','DVLE_MonthEnd')
            VLE_DVLE_Joined = df2.select('DimSetID','VLE_No','DVLE_Posting_Date','DocumentDate','Due_Date','PaymentTermsCode','CurrencyCode','ExternalDocumentNo','DocumentType','Remaining_Amount','Original_Amount')
            cond = [sqldf.TempVLE_No == VLE_DVLE_Joined.VLE_No]
            APsnapshots = LJOIN(sqldf,VLE_DVLE_Joined,cond)# no yeear Month
            APsnapshots=APsnapshots.select('DimSetID','TempVLE_No','DVLE_Posting_Date','DocumentDate','Due_Date','PaymentTermsCode','CurrencyCode','ExternalDocumentNo','DocumentType','Remaining_Amount','Original_Amount','VLE_Document_No','DVLE_MonthEnd').filter(APsnapshots['DVLE_Posting_Date']<= APsnapshots['DVLE_MonthEnd'])
            APsnapshots = APsnapshots.groupBy('DimSetID','TempVLE_No','VLE_Document_No','DVLE_MonthEnd','DocumentDate','Due_Date','PaymentTermsCode','CurrencyCode','ExternalDocumentNo','DocumentType').agg({'Remaining_Amount':'sum','Original_Amount':'sum'}).withColumnRenamed('sum(Remaining_Amount)','Remaining_Amount').withColumnRenamed('sum(Original_Amount)','Original_Amount')
            VLE_DVLE_Joined = df2.select('VLE_No','LinkVendor','VLE_Posting_Date','LinkPurchaser').distinct()
            
            cond = [APsnapshots.TempVLE_No == VLE_DVLE_Joined.VLE_No]
            APsnapshots = LJOIN(APsnapshots,VLE_DVLE_Joined,cond)
           
            APsnapshots = APsnapshots.withColumn('Due_Date',APsnapshots['Due_Date'].cast('date'))\
                                .withColumn('DVLE_MonthEnd',APsnapshots['DVLE_MonthEnd'].cast('date'))\
                                .withColumn('VLE_Posting_Date',APsnapshots['VLE_Posting_Date'].cast('date'))
            APsnapshots = APsnapshots.withColumn("Document_No",APsnapshots['VLE_Document_No'])\
                                .withColumn("Link_Date",APsnapshots['DVLE_MonthEnd'].cast('date'))\
                                .withColumn("NOD_AP_Due_Date",datediff(APsnapshots['DVLE_MonthEnd'],APsnapshots['Due_Date']))\
                                .withColumn("NOD_AP_Posting_Date",datediff(APsnapshots['DVLE_MonthEnd'],APsnapshots['VLE_Posting_Date']))\
                                .withColumn("NOD_AP_Document_Date",datediff(APsnapshots['DVLE_MonthEnd'],APsnapshots['DocumentDate']))\
                                .withColumn("TransactionType",lit('VLE_Entry'))\
                                .withColumn("AP_Type",when(APsnapshots['Remaining_Amount']<0, lit('Adv/UnAdj')).otherwise(lit('AP')))
            
            APsnapshots.cache()
            print(APsnapshots.count())
            APBucket=spark.read.format("parquet").load(STAGE1_Configurator_Path+"/tblAPBucket").drop("DBName","EntityName")
            Maxoflt = APBucket.filter(APBucket['BucketName']=='<')
            MaxLimit = int(Maxoflt.select('UpperLimit').first()[0])
            Minofgt = APBucket.filter(APBucket['BucketName']=='>')
            MinLimit = int(Minofgt.select('LowerLimit').first()[0])
  
            APsnapshots = APsnapshots.join(APBucket,APsnapshots.NOD_AP_Posting_Date == APBucket.Nod,'left').drop('ID','UpperLimit','LowerLimit')
            APsnapshots=APsnapshots.withColumn('BucketName',when(APsnapshots.NOD_AP_Posting_Date>=MinLimit,lit(str(MinLimit)+'+')).otherwise(APsnapshots.BucketName))\
                        .withColumn('Nod',when(APsnapshots.NOD_AP_Posting_Date>=MinLimit,APsnapshots.NOD_AP_Posting_Date).otherwise(APsnapshots.Nod))
            APsnapshots=APsnapshots.withColumn('BucketName',when(APsnapshots.NOD_AP_Posting_Date<=(MaxLimit),lit("Not Due")).otherwise(APsnapshots.BucketName))\
                        .withColumn('Nod',when(APsnapshots.NOD_AP_Posting_Date<=(MaxLimit), APsnapshots.NOD_AP_Posting_Date).otherwise(APsnapshots.Nod)) 
            APsnapshots = APsnapshots.withColumnRenamed('DimSetID','DimensionSetID')
            
            finalDF = APsnapshots.join(DSE,"DimensionSetID",'left')
            finalDF = RenameDuplicateColumns(finalDF)
            finalDF.cache()
            finalDF .coalesce(1).write.format("parquet").mode("overwrite").option("overwriteSchema", "true").save(STAGE2_PATH+"/"+"Purchase/Payables_Snapshot")
            logger.endExecution()
            
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"
            log_dict = logger.getSuccessLoggedRecord("Purchase.Payables_Snapshot", DBName, EntityName, finalDF.count(), len(finalDF.columns), IDEorBatch)
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
            os.system("spark-submit "+Kockpit_Path+"/Email.py 1 Payables_Snapshot '"+CompanyName+"' "+DBEntity+" "+str(exc_traceback.tb_lineno)+" ")
            
            log_dict = logger.getErrorLoggedRecord('Purchase.Payables_Snapshot', DBName, EntityName, str(ex), str(exc_traceback.tb_lineno), IDEorBatch)
            log_df = spark.createDataFrame(log_dict, logger.getSchema())
            log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)
print('purchase_Payables_Snapshot completed: ' + str((dt.datetime.now()-st).total_seconds()))


    