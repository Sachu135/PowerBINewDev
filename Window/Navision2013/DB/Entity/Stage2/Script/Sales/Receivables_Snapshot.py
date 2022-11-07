from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.functions import col,concat,year,when,month,to_date,lit,sum,last_day,datediff
import datetime as dt 
import os,sys,datetime
from datetime import timedelta
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
FilePathSplit = Filepath.split('\\')
DBName = FilePathSplit[-5]
EntityName = FilePathSplit[-4]
DBEntity = DBName+EntityName

STAGE1_Configurator_Path=Kockpit_Path+"/" +DBName+"/" +EntityName+"/" +"Stage1/ConfiguratorData/"
STAGE1_PATH=Kockpit_Path+"/" +DBName+"/" +EntityName+"/" +"Stage1/ParquetData"
STAGE2_PATH=Kockpit_Path+"/" +DBName+"/" +EntityName+"/" +"Stage2/ParquetData"
conf = SparkConf().setMaster("local[*]").setAppName("Receivables_Snapshot").\
                    set("spark.sql.shuffle.partitions",16).\
                    set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").\
                    set("spark.local.dir", "/tmp/spark-temp").\
                    set("spark.driver.memory","30g").\
                    set("spark.executor.memory","30g").\
                    set("spark.driver.cores",'*').\
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
for dbe in config["DbEntities"]:
    if dbe['ActiveInactive']=='true' and  dbe['Location']==DBEntity:
        CompanyName=dbe['Name']
        CompanyName=CompanyName.replace(" ","")
        try:
            logger = Logger()
            cle =spark.read.format("parquet").load(STAGE1_PATH+"/Cust_ Ledger Entry")
            dcle = spark.read.format("parquet").load(STAGE1_PATH+"/Detailed Cust_ Ledg_ Entry")
            sih=spark.read.format("parquet").load(STAGE1_PATH+"/Sales Invoice Header")
            cpg =spark.read.format("parquet").load(STAGE1_PATH+"/Customer Posting Group")
            DSE=spark.read.format("parquet").load(STAGE1_PATH+"/"+"../../"+"Stage2/ParquetData/Masters/DSE").drop("DBName","EntityName")
            CD=spark.read.format("parquet").load(STAGE1_PATH+"/Collection Details")
            GLRange=spark.read.format("parquet").load(STAGE1_Configurator_Path+"/tblGLAccountMapping")
            Company =spark.read.format("parquet").load(STAGE1_Configurator_Path+"/tblCompanyName")
            ARBucket =spark.read.format("parquet").load(STAGE1_Configurator_Path+"/tblARBucket")
            pdm =spark.read.format("parquet").load(STAGE1_PATH+"/Payment Delay Masters")
            doc=spark.read.format("parquet").load(STAGE1_PATH+"/Documentation")
            Datelog = datetime.datetime.now().strftime('%Y-%m-%d')
            cle = cle.select('CustomerNo_','SalespersonCode','DueDate','EntryNo_','DocumentNo_','PostingDate','CustomerPostingGroup','DimensionSetID','ExternalDocumentNo_','DocumentType','CurrencyCode','AdvanceCollection')
            dcle = dcle.select('AmountLCY','Amount','PostingDate','EntryType','Cust_LedgerEntryNo_','DocumentNo_')
            sih = sih.select('No_','PaymentTermsCode')
            cpg = cpg.select('Code','ReceivablesAccount')
            dcle = dcle.withColumn('AmountLCY',dcle['AmountLCY'].cast('decimal(20,4)')).withColumn('Amount',dcle['Amount'].cast('decimal(20,4)'))    
            cle = cle.withColumn("Link_Customer",when(col("CustomerNo_")=='',"NA").otherwise(col("CustomerNo_")))\
                     .withColumn("Link_SalesPerson",when(col("SalespersonCode")=='',"NA").otherwise(col("SalespersonCode")))\
                     .withColumn("Due_Date",to_date(col("DueDate")))
            cle = cle.drop('CustomerNo_','SalespersonCode')
            cle = Kockpit.RENAME(cle,{'EntryNo_':'CLE_No','DocumentNo_':'CLE_Document_No','PostingDate':'CLE_Posting_Date','CustomerPostingGroup':'Customer_Posting_Group',\
                        'DimensionSetID':'DimSetID','ExternalDocumentNo_':'ExternalDocumentNo'})    
            cle = cle.join(sih,cle['CLE_Document_No']==sih['No_'],'left')
            cle = cle.drop('No_')    
            dcle = dcle.filter(year(col("PostingDate"))!='1753')
            dcle = dcle.withColumn("YearMonth",concat(year(dcle.PostingDate),month(dcle.PostingDate))).withColumn("Today",lit(Datelog))
            dcle = dcle.withColumn("TodayYM",concat(year(dcle.Today),month(dcle.Today)))
            dcle = dcle.withColumn("Original_Amount",when(col("EntryType")==1,col("Amount")))\
                       .withColumn("DCLE_Posting_Date",to_date(col("PostingDate")))\
                       .withColumn("DCLE_Monthend_Posting_Date",when(dcle.YearMonth==dcle.TodayYM, dcle.Today).otherwise(last_day(col("PostingDate"))))\
                       .withColumn("Transaction_Type",lit("CLE Entry"))\
                       .withColumn("Remaining_Amount",col("AmountLCY"))
            dcle = dcle.drop("Today","TodayYM","Amount","PostingDate","AmountLCY","EntryType","YearMonth")
            dcle = Kockpit.RENAME(dcle,{'Cust_LedgerEntryNo_':'DCLE_No','DocumentNo_':'DCLE_Document_No','EntryNo_':'DCLE_No'})   
            df = dcle.join(cle,cle['CLE_No']==dcle['DCLE_No'],'left')  
            df1 = Kockpit.RENAME(cpg,{'Code':'Customer_Posting_Group','ReceivablesAccount':'GLAccount'})    
            df2 = df.join(df1,'Customer_Posting_Group','left')    
            GLRange = GLRange.filter(GLRange['DBName'] == DBName ).filter(GLRange['EntityName'] == EntityName ).filter(GLRange['GLRangeCategory']== 'Customer')
            GLRange = Kockpit.RENAME(GLRange,{'FromGLCode':'FromGL','ToGLCode':'ToGL','GLRangeCategory':'GLCategory'})
            GLRange = GLRange.select("GLCategory","FromGL","ToGL")
            Range=df2['GLAccount']!=0
            NoOfRows=GLRange.count()    
            for i in range(0,NoOfRows):
                if i==0:
                        FromGL = "%s"%GLRange.select('FromGL').collect()[0]["FromGL"]
                        ToGL = "%s"%GLRange.select('ToGL').collect()[0]["ToGL"]
                        Range = (df2['GLAccount']>=FromGL) & (df2['GLAccount']<=ToGL)
                else:
                        FromGL = "%s"%GLRange.select('FromGL').collect()[i]["FromGL"]
                        ToGL = "%s"%GLRange.select('ToGL').collect()[i]["ToGL"]
                        Range = (Range) | ((df2['GLAccount']>=FromGL) & (df2['GLAccount']<=ToGL))
            
            df2 = df2.filter(Range)
            df4 = df2.groupBy('CLE_No').agg({'Remaining_Amount':'sum'})
            df4 = Kockpit.RENAME(df4,{'sum(Remaining_Amount)':'Remaining_Amount','CLE_No':'CLENo'})
            df4 = df4.filter(df4['Remaining_Amount'] != 0)
            df_DCLE_Temp1=df.select("CLE_No","CLE_Document_No","DCLE_Monthend_Posting_Date")    
            Df_min_max_date = df_DCLE_Temp1.join(df4,df_DCLE_Temp1.CLE_No==df4.CLENo,'left').distinct()
            Df_min_max_date = Df_min_max_date.select("CLE_No","CLE_Document_No","DCLE_Monthend_Posting_Date","Remaining_Amount")   
            sqldf = Df_min_max_date.withColumn('Max_MonthEnd',when(Df_min_max_date['Remaining_Amount']!=0, datetime.datetime.now().date().replace(month=12, day=31))\
                    .otherwise(Df_min_max_date['DCLE_Monthend_Posting_Date']))
            sqldf = sqldf.groupby('CLE_No','CLE_Document_No','Remaining_Amount').agg({'DCLE_Monthend_Posting_Date':'min','Max_MonthEnd':'max'})
            sqldf = Kockpit.RENAME(sqldf,{'CLE_No':'TempCLE_No','min(DCLE_Monthend_Posting_Date)':'Min_MonthEnd','max(Max_MonthEnd)':'Max_MonthEnd'})
            sqldf.cache()
            print(sqldf.count())
            df = Company.filter(col('DBName')==DBName).filter(col('NewCompanyName') == EntityName)
            df = df.select("StartDate","EndDate")
            Calendar_StartDate = df.select(df.StartDate).collect()[0]["StartDate"]
            Calendar_StartDate = datetime.datetime.strptime(Calendar_StartDate,'%m/%d/%Y').date()
            
            if datetime.date.today().month>int(MnSt)-1:
                    UIStartYr=datetime.date.today().year-int(yr)+1
            else:
                    UIStartYr=datetime.date.today().year-int(yr)
            UIStartDate=datetime.date(UIStartYr,int(MnSt),1)
            UIStartDate=max(Calendar_StartDate,UIStartDate)
            
            cdate = datetime.datetime.now().strftime("%m/%d/%Y")
            Calendar_EndDate_conf=df.select(df.EndDate).collect()[0]["EndDate"]
            Calendar_EndDate_conf = datetime.datetime.strptime(Calendar_EndDate_conf,'%m/%d/%Y').date()
            Calendar_EndDate_file=datetime.datetime.strptime(cdate,"%m/%d/%Y").date()
            Calendar_EndDate=min(Calendar_EndDate_conf,Calendar_EndDate_file)
            days = (Calendar_EndDate-UIStartDate).days   
            def last_day_of_month(date):
                    if date.month == 12:
                            return date.replace(day=31)
                    return date.replace(month=date.month+1, day=1) - datetime.timedelta(days=1)
            
            def daterange(start_date,end_date):
                    for n in range(int ((end_date - start_date).days)):
                            yield start_date + timedelta(n)
            
            data =[]
            
            for single_date in daterange(UIStartDate, Calendar_EndDate+timedelta(days=1)):
                    data.append({'Link_date':single_date})
                    
            schema = StructType([StructField("Link_date", DateType(),True)])
            records=spark.createDataFrame(data,schema)
            records=records.select(last_day(records.Link_date).alias('Link_date')).distinct().sort('Link_date')
            records=records.withColumn("Link_date", \
                                      when(records["Link_date"] == last_day_of_month(Calendar_EndDate), Calendar_EndDate).otherwise(records["Link_date"]))
            records.cache()
            print(records.count())
            sqldf = sqldf.crossJoin(records)
            sqldf.cache()
            print(sqldf.count())
            
            sqldf = sqldf.select('TempCLE_No','CLE_Document_No','Min_MonthEnd','Max_MonthEnd','Link_date')
            sqldf=sqldf.filter(sqldf['Link_date']<= sqldf['Max_MonthEnd']).filter(sqldf['Link_date']>= sqldf['Min_MonthEnd'])\
                       .select('TempCLE_No','CLE_Document_No','Link_date')
            sqldf = Kockpit.RENAME(sqldf,{'Link_date':'DCLE_MonthEnd'})
            
            CLE_DCLE_Joined = df2.select('DimSetID','CLE_No','DCLE_Posting_Date','Due_Date','DocumentType','ExternalDocumentNo','CurrencyCode','PaymentTermsCode','Remaining_Amount','Original_Amount')
            ARsnapshots = sqldf.join(CLE_DCLE_Joined,sqldf.TempCLE_No == CLE_DCLE_Joined.CLE_No,'left')
            ARsnapshots=ARsnapshots.select('DimSetID','TempCLE_No','DCLE_Posting_Date','Due_Date','DocumentType','ExternalDocumentNo','CurrencyCode','PaymentTermsCode','Remaining_Amount','Original_Amount','CLE_Document_No','DCLE_MonthEnd')
            ARsnapshots=ARsnapshots.filter(ARsnapshots['DCLE_Posting_Date']<= ARsnapshots['DCLE_MonthEnd'])                  
            ARsnapshots = ARsnapshots.groupBy('DimSetID','TempCLE_No','CLE_Document_No','DCLE_MonthEnd','Due_Date','DocumentType','ExternalDocumentNo','PaymentTermsCode','CurrencyCode').agg({'Remaining_Amount':'sum','Original_Amount':'sum'})
            ARsnapshots = Kockpit.RENAME(ARsnapshots,{'sum(Remaining_Amount)':'Remaining_Amount','sum(Original_Amount)':'Original_Amount'})
            ARsnapshots.cache()
            print(ARsnapshots.count())
            
            CLE_DCLE_Joined = df2.select('CLE_No','Link_Customer','CLE_Posting_Date','Link_SalesPerson','AdvanceCollection').distinct()
            ARsnapshots = ARsnapshots.join(CLE_DCLE_Joined,ARsnapshots['TempCLE_No']==CLE_DCLE_Joined['CLE_No'],'left')
           
                    
            ARsnapshots = ARsnapshots.withColumn("NOD_AR_Due_Date",datediff(ARsnapshots['DCLE_MonthEnd'],ARsnapshots['Due_Date']))\
                                .withColumn("NOD_AR_Posting_Date",datediff(ARsnapshots['DCLE_MonthEnd'],ARsnapshots['CLE_Posting_Date']))\
                                .withColumn("TransactionType",lit('CLE_Entry'))\
                                .withColumn("AR_AP_Type",when(ARsnapshots['Remaining_Amount']<0, lit('Adv/UnAdj')).otherwise(lit('AR')))
            ARsnapshots = Kockpit.RENAME(ARsnapshots,{'CLE_Document_No':'Document_No','DCLE_MonthEnd':'Link_Date'})
            Maxoflt = ARBucket.filter(ARBucket['BucketName']=='<')
            MaxLimit = int(Maxoflt.select('UpperLimit').first()[0])
            Minofgt = ARBucket.filter(ARBucket['BucketName']=='>')
            MinLimit = int(Minofgt.select('LowerLimit').first()[0])
  
            ARsnapshots = ARsnapshots.join(ARBucket,ARsnapshots.NOD_AR_Posting_Date == ARBucket.Nod,'left').drop('ID','UpperLimit','LowerLimit')
            ARsnapshots=ARsnapshots.withColumn('BucketName',when(ARsnapshots.NOD_AR_Posting_Date>=MinLimit,lit(str(MinLimit)+'+')).otherwise(ARsnapshots.BucketName))\
                        .withColumn('Nod',when(ARsnapshots.NOD_AR_Posting_Date>=MinLimit,ARsnapshots.NOD_AR_Posting_Date).otherwise(ARsnapshots.Nod))
            ARsnapshots=ARsnapshots.withColumn('BucketName',when(ARsnapshots.NOD_AR_Posting_Date<=(MaxLimit),lit("Not Due")).otherwise(ARsnapshots.BucketName))\
                        .withColumn('Nod',when(ARsnapshots.NOD_AR_Posting_Date<=(MaxLimit), ARsnapshots.NOD_AR_Posting_Date).otherwise(ARsnapshots.Nod))
            ARsnapshots = Kockpit.RENAME(ARsnapshots,{'Link_Customer':'LinkCustomer','Link_SalesPerson':'LinkSalesPerson'})
            ARsnapshots = ARsnapshots.withColumn("AdvanceFlag",when(ARsnapshots["AdvanceCollection"]==1,lit('Advance')).otherwise(lit('NA')))\
                                    .withColumnRenamed('DimSetID','DimensionSetID')
            ARsnapshots = ARsnapshots.join(DSE,"DimensionSetID",'left')
            finalDF = RenameDuplicateColumns(ARsnapshots)
            finalDF.coalesce(1).write.format("parquet").mode("overwrite").option("overwriteSchema", "true").save(STAGE2_PATH+"/"+"Sales/Receivables_Snapshot")
          
            logger.endExecution()
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"
            log_dict = logger.getSuccessLoggedRecord("Sales.Receivables_Snapshot", DBName, EntityName, finalDF.count(), len(finalDF.columns), IDEorBatch)
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
            os.system("spark-submit "+Kockpit_Path+"/Email.py 1 Receivables_Snapshot '"+CompanyName+"' "+DBEntity+" "+str(exc_traceback.tb_lineno)+" ")
            log_dict = logger.getErrorLoggedRecord('Sales.Receivables_Snapshot', DBName, EntityName, str(ex), str(exc_traceback.tb_lineno), IDEorBatch)
            log_df = spark.createDataFrame(log_dict, logger.getSchema())
            log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)
print('sales_Receivables_Snapshot completed: ' + str((dt.datetime.now()-st).total_seconds()))