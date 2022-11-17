from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import io,os,datetime
from datetime import date
from pyspark.sql.functions import col,concat,concat_ws,year,when,month,to_date,lit,expr,sum,count,split,last_day,explode,trunc
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import datetime,time,sys
from pyspark.sql.types import *
from builtins import str
import traceback
from os.path import dirname, join, abspath
import datetime as dt
from datetime import datetime
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
FilePathSplit = Filepath.split('/')
DBName = FilePathSplit[-5]
EntityName = FilePathSplit[-4]
DBEntity = DBName+EntityName
entityLocation = DBName+EntityName
STAGE1_Configurator_Path=HDFS_PATH+DIR_PATH+"/" +DBName+"/" +EntityName+"/" +"Stage1/ConfiguratorData/"
STAGE1_PATH=HDFS_PATH+DIR_PATH+"/" +DBName+"/" +EntityName+"/" +"Stage1/ParquetData"
STAGE2_PATH=HDFS_PATH+DIR_PATH+"/" +DBName+"/" +EntityName+"/" +"Stage2/ParquetData"
conf = SparkConf().setMaster(SPARK_MASTER).setAppName("CashFlow")\
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
            GL_Entry_Table =spark.read.format("delta").load(STAGE1_PATH+"/G_L Entry")
            Company =spark.read.format("delta").load(STAGE1_Configurator_Path+"/tblCompanyName")
            FALedgerEntry_Table =spark.read.format("delta").load(STAGE1_PATH+"/FA Ledger Entry")
            GL_Account_Table=spark.read.format("delta").load(STAGE1_PATH+"/G_L Account")
            DSE =DSE=spark.read.format("delta").load(STAGE2_PATH+"/"+"Masters/DSE")
            COA_Table =spark.read.format("delta").load(STAGE1_Configurator_Path+"/ChartofAccounts")
            COA_Table = COA_Table.filter(COA_Table['DBName']==DBName).filter(COA_Table['EntityName']==EntityName)
            COA_Table = COA_Table.select('GLAccountNo','CFReportHeader','AccountDescription','IsNegativePolarity')
            COA_Table = COA_Table.withColumnRenamed('GLAccountNo','GLAccount').withColumnRenamed('CFReportHeader','CFReportFlag')\
                                .withColumnRenamed('AccountDescription','Description')
            COA_Table = COA_Table.withColumn('IsNegativePolarity',COA_Table['IsNegativePolarity'].cast('string'))
            COA_Table=COA_Table.filter(COA_Table["GLAccount"]!='SERVER')
            CashBeginning = COA_Table.filter(COA_Table['CFReportFlag'].isin(['Opening Funds']))
            GL_List_CashBeginning = [i.GLAccount for i in CashBeginning.select('GLAccount').distinct().collect()]
            CashClosing = COA_Table.filter(COA_Table['CFReportFlag'].isin(['Bank Balances','FDs Pledged to Banks','FDs which are Free']))
            GL_List_CashClosing = [i.GLAccount for i in CashClosing.select('GLAccount').distinct().collect()]                    
                                
            duplicates = COA_Table.withColumn('DuplicateCOA',split(COA_Table['GLAccount'],'_')[1])\
                        .withColumn('MappingGL',split(COA_Table['GLAccount'],'_')[0])
            
            Company = Company.filter(col('DBName')==DBName).filter(col('NewCompanyName') == EntityName)
            Calendar_StartDate = Company.select('StartDate').rdd.flatMap(lambda x: x).collect()[0]
            Calendar_StartDate = datetime.datetime.strptime(Calendar_StartDate,'%Y-%m-%d').date()
            if datetime.date.today().month>int(MnSt)-1:
                UIStartYr=datetime.date.today().year-int(yr)+1
            else:
                UIStartYr=datetime.date.today().year-int(yr)
            UIStartDate=datetime.date(UIStartYr,int(MnSt),1)
            UIStartDate=max(Calendar_StartDate,UIStartDate)
            FALedgerEntry_Table = FALedgerEntry_Table.select('FAPostingType','DisposalEntryNo_','PostingDate','AmountLCY',
                                                     'DocumentNo_','DimensionSetID')
            FALedgerEntry_Table = FALedgerEntry_Table.withColumnRenamed('AmountLCY','Amount').withColumnRenamed('DocumentNo_','Document_No')
            FALedgerEntry_Table = FALedgerEntry_Table.filter(((FALedgerEntry_Table['FAPostingType']==0) & (FALedgerEntry_Table['DisposalEntryNo_']==0))
                                                             |(FALedgerEntry_Table['FAPostingType']==6))
            
            GL_List = [int(i.GLAccount) for i in COA_Table.select('GLAccount').distinct().collect()]
            Dummy_GL_OPENINGCASH = max(GL_List)*100+10
            Dummy_GL_FAPurchase = max(GL_List)*100+20
            Dummy_GL_FADisposal = max(GL_List)*100+30
            
            GL_Mapping =spark.read.format("delta").load(STAGE1_Configurator_Path+"/tblGLAccountMapping")
            
            OpeningCash_GL = GL_Mapping.filter(GL_Mapping['GLRangeCategory']=='Cash & Cash Equivalents')
            OpeningCash_GL = OpeningCash_GL.select('FromGLCode','ToGLCode')
            
            OpeningCash_GL.cache()
            OpeningCash_GL_List = []
            NoofRows = OpeningCash_GL.count()
            for i in range(0,NoofRows):
                FromGL = int(OpeningCash_GL.select('FromGLCode').collect()[i]['FromGLCode'])
                ToGL = int(OpeningCash_GL.select('ToGLCode').collect()[i]['ToGLCode'])
                
                for i in range(FromGL,ToGL+1,1):
                    if i in GL_List:
                        OpeningCash_GL_List.append(i)
            
            GL_Account_Table = GL_Account_Table.select("No_","Income_Balance")\
                                    .withColumnRenamed('No_','No').drop('DBName','EntityName')
            GL_Entry_Table = GL_Entry_Table.withColumn('PostingDate',to_date(GL_Entry_Table['PostingDate'])).drop('DBName','EntityName')
            GLEntry = GL_Entry_Table.join(GL_Account_Table,GL_Entry_Table['G_LAccountNo_']==GL_Account_Table['No'],'left')
            GLEntry = GLEntry.filter(year(GLEntry["PostingDate"])!='1753').filter(GLEntry['SourceCode']!='CLSINCOME')
            GLEntry = GLEntry.withColumnRenamed("EntryNo_","Entry_No")\
                                        .withColumn("LinkDate",GL_Entry_Table["PostingDate"])\
                                        .withColumn("GL_Posting_Date",GL_Entry_Table["PostingDate"])\
                                        .withColumn("linkdoc", concat(GL_Entry_Table["DocumentNo_"],lit('||'),GL_Entry_Table["G_LAccountNo_"]))\
                                        .withColumn("Cost_Amount",GL_Entry_Table["Amount"])\
                                        .withColumnRenamed("DocumentNo_","Document_No")\
                                        .withColumnRenamed("Description","GL_Description")\
                                        .withColumnRenamed("SourceNo_","Source_No")\
                                        .withColumnRenamed("G_LAccountNo_","GLAccount")\
                                        .withColumn("Transaction_Type",lit("GLEntry"))\
                                        .withColumn("Entry_Flag",lit("GL"))
            GLEntry = GLEntry.join(COA_Table,'GLAccount','left')
            GLEntry = GLEntry.withColumn('Amount',GLEntry['Amount'].cast('decimal(30,4)'))
            ClosingCash_GLEntry = GLEntry.filter(GLEntry['GLAccount'].isin(GL_List_CashClosing))
            ClosingCash_Grouped = ClosingCash_GLEntry.withColumn('FY Year',when(month(ClosingCash_GLEntry['LinkDate'])>=4,year(ClosingCash_GLEntry['LinkDate']))\
                                                                 .otherwise(year(ClosingCash_GLEntry['LinkDate'])-1))
            ClosingCash_Grouped = ClosingCash_Grouped.withColumn('Month',when(month(ClosingCash_GLEntry['LinkDate'])>=4,month(ClosingCash_GLEntry['LinkDate'])-3)\
                                                                 .otherwise(month(ClosingCash_GLEntry['LinkDate'])+9))
            YM = str(datetime.datetime.today().year) + str(datetime.datetime.today().month)
            ClosingCash_Grouped = ClosingCash_Grouped.withColumn('LinkDate',
                                                    when(concat(year(ClosingCash_Grouped['LinkDate'])
                                                    ,month(ClosingCash_Grouped['LinkDate']))==YM,datetime.datetime.today().date())\
                                                    .otherwise(last_day(ClosingCash_Grouped['LinkDate'])))
            
            GL_Month = ClosingCash_Grouped.groupBy('GLAccount').agg({'LinkDate':'min'})\
                                        .withColumnRenamed('min(LinkDate)','MinDate')
            GL_Month = GL_Month.withColumn('MinDate', trunc('MinDate', 'month'))
            GL_Month = GL_Month.withColumn('MaxDate',lit(datetime.datetime.today().date()))
            GL_Month = GL_Month.withColumn('LinkDate', explode(expr('sequence(MinDate, MaxDate, interval 1 month)')))
            GL_Month = GL_Month.withColumn('LinkYM',concat(year(GL_Month['LinkDate']),month(GL_Month['LinkDate'])))
            GL_Month = GL_Month.withColumn('LinkDate',last_day(GL_Month['LinkDate']))
            GL_Month = GL_Month.withColumn('LinkDate',when(GL_Month['LinkYM']==YM,GL_Month['MaxDate']).otherwise(GL_Month['LinkDate']))
            GL_Month = GL_Month.select('GLAccount','LinkDate')
            ClosingCash_Grouped = ClosingCash_Grouped.groupBy('GLAccount','LinkDate').agg({'Amount':'sum'}).withColumnRenamed('sum(Amount)','Amount')
            ClosingCash_Grouped = ClosingCash_Grouped.join(GL_Month,['GLAccount','LinkDate'],'right')
            win_spec = Window.partitionBy('GLAccount').orderBy('LinkDate').rowsBetween(-sys.maxsize, 0)
            win_spec_lag = Window.partitionBy('GLAccount').orderBy('LinkDate')
            ClosingCash_Grouped = ClosingCash_Grouped.withColumn('Amount',F.sum('Amount').over(win_spec))
            ClosingCash_Grouped = ClosingCash_Grouped.withColumn('Amount',ClosingCash_Grouped['Amount'].cast('decimal(30,4)'))
            ClosingCash_GLEntry = ClosingCash_Grouped.withColumn('LinkDate',to_date(ClosingCash_Grouped['LinkDate']))
            duplicates = duplicates.filter(~(duplicates['DuplicateCOA'].isNull()))
            duplicates = duplicates.withColumn('Polarity',when(duplicates['IsNegativePolarity']==True, lit(-1)).otherwise(lit(1)))
            NoofType = [i.DuplicateCOA for i in duplicates.select('DuplicateCOA').distinct().collect()]
            for index,i in enumerate(NoofType):
                p = duplicates.filter(duplicates['DuplicateCOA']==i)
                flagging = p.select('GLAccount','Polarity').distinct()
                p.cache()
                list_gl = [int(i.MappingGL) for i in p.select('MappingGL').distinct().collect()]
                GLEntry_Dummy = GLEntry.filter(GLEntry['GLAccount'].isin(list_gl))
                GLEntry_Dummy = GLEntry_Dummy.withColumn('GLAccount',concat_ws('_',GLEntry_Dummy['GLAccount'],lit(i)))
                GLEntry_Dummy = GLEntry_Dummy.join(flagging,'GLAccount','left')
                GLEntry_Dummy = GLEntry_Dummy.withColumn('Amount',GLEntry_Dummy['Amount']*GLEntry_Dummy['Polarity'])
                if index==0:
                    Duplicate_GLEntry = GLEntry_Dummy
                else:
                    Duplicate_GLEntry = CONCATENATE(Duplicate_GLEntry,GLEntry_Dummy,spark)    
            Duplicate_GLEntry = Duplicate_GLEntry.withColumn('Income_Balance',lit('5'))
            Duplicate_GLEntry = Duplicate_GLEntry.withColumn('Amount',Duplicate_GLEntry['Amount'].cast('decimal(30,4)'))
            OpeningCash_GLEntry = Duplicate_GLEntry.filter(Duplicate_GLEntry['GLAccount'].isin(GL_List_CashBeginning))
            Duplicate_GLEntry = Duplicate_GLEntry.filter(~(Duplicate_GLEntry['GLAccount'].isin(GL_List_CashBeginning)))
            if datetime.datetime.now().month>=4:
                Year = datetime.datetime.now().year
                YTDDate = str(Year)+'-03-31'
            else:
                Year = datetime.datetime.now().year-1
                YTDDate = str(Year)+'-03-31'
            OpeningCash_Grouped = OpeningCash_GLEntry.withColumn('FY Year',when(month(OpeningCash_GLEntry['LinkDate'])>=4,year(OpeningCash_GLEntry['LinkDate']))\
                                                                 .otherwise(year(OpeningCash_GLEntry['LinkDate'])-1))
            OpeningCash_Grouped = OpeningCash_Grouped.groupBy('GLAccount','FY Year').agg({'Amount':'sum'}).withColumnRenamed('sum(Amount)','Amount')
            win_spec = Window.partitionBy('GLAccount').orderBy('FY Year').rowsBetween(-sys.maxsize, 0)
            win_spec_lag = Window.partitionBy('GLAccount').orderBy('FY Year')
            OpeningCash_Grouped = OpeningCash_Grouped.withColumn('Amount',F.sum('Amount').over(win_spec))
            OpeningCash_Grouped = OpeningCash_Grouped.withColumn('Amount',F.lag('Amount').over(win_spec_lag))
            OpeningCash_Grouped = OpeningCash_Grouped.na.fill({'Amount':0})
            OpeningCash_Grouped = OpeningCash_Grouped.withColumn('Amount',OpeningCash_Grouped['Amount'].cast('decimal(30,4)'))
            OpeningCash_GLEntry = OpeningCash_Grouped.withColumn('LinkDate',concat(OpeningCash_Grouped['FY Year'],lit('-04-01')))                                      
            OpeningCash_GLEntry = OpeningCash_GLEntry.drop('FY Year')\
                                                    .withColumn('LinkDate',to_date(OpeningCash_GLEntry['LinkDate']))
            GLEntry = GLEntry.filter(~(GLEntry['GLAccount'].isin(GL_List_CashClosing)))
            GLEntry = CONCATENATE( GLEntry,OpeningCash_GLEntry, spark)
            GLEntry = CONCATENATE(GLEntry,ClosingCash_GLEntry,spark)
            GLEntry = CONCATENATE(GLEntry,Duplicate_GLEntry, spark)
            GLEntry =  GLEntry.join(DSE,"DimensionSetID",'left')  
            GLEntry=GLEntry.withColumn("DBName",concat(lit(DBName))).withColumn("EntityName",concat(lit(EntityName)))
            GLEntry = GLEntry.withColumn("LinkDateKey",concat_ws("|",lit(DBName),lit(EntityName),GLEntry.LinkDate))\
                    .withColumn("Link_GLAccount_Key",concat_ws("|",lit(DBName),lit(EntityName),GLEntry.GLAccount))\
                    .withColumn("DBName",lit(DBName))\
                    .withColumn("EntityName",lit(EntityName))               
            GLEntry = GLEntry.groupBy('LinkDate','GLAccount',"Link_GLAccount_Key",'LinkDateKey',
                                      'DBName','EntityName','Income_Balance').agg({'Amount':'sum'})\
                                .withColumnRenamed('sum(Amount)','Amount')                
            GLEntry.coalesce(1).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(STAGE2_PATH+"/"+"Finance/CashFlow")
            logger.endExecution()
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"
        
            log_dict = logger.getSuccessLoggedRecord("Finance.CashFlow", DBName, EntityName, GLEntry.count(), len(GLEntry.columns), IDEorBatch)
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
            os.system("spark-submit "+Kockpit_Path+"/Email.py 1 CashFlow '"+CompanyName+"' "+DBEntity+" "+str(exc_traceback.tb_lineno)+" ")
    
            log_dict = logger.getErrorLoggedRecord('Finance.CashFlow', DBName, EntityName, str(ex), str(exc_traceback.tb_lineno), IDEorBatch)
            log_df = spark.createDataFrame(log_dict, logger.getSchema())
            log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)
print('finance_CashFlow completed: ' + str((dt.datetime.now()-st).total_seconds()))  
 

