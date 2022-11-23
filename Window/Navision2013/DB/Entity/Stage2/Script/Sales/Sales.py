from pyspark.sql import SparkSession,SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import to_date,concat_ws,count,when,lit,sum,concat, col
from pyspark.sql.types import *
import os,sys
from os.path import dirname, join, abspath
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

Filepath = os.path.dirname(os.path.abspath(__file__))
FilePathSplit = Filepath.split('\\')
DBName = FilePathSplit[-5]
EntityName = FilePathSplit[-4]
DBEntity = DBName+EntityName
STAGE1_Configurator_Path=Kockpit_Path+"/" +DBName+"/" +EntityName+"/" +"Stage1/ConfiguratorData/"
STAGE1_PATH=Kockpit_Path+"/" +DBName+"/" +EntityName+"/" +"Stage1/ParquetData"
STAGE2_PATH=Kockpit_Path+"/" +DBName+"/" +EntityName+"/" +"Stage2/ParquetData"
DBNamepath= abspath(join(join(dirname(__file__), '..'),'..','..','..'))
conf = SparkConf().setMaster("local[*]").setAppName("Sales").\
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
cy = date.today().year
cm = date.today().month

for dbe in config["DbEntities"]:
    if dbe['ActiveInactive']=='true' and  dbe['Location']==DBEntity:
        CompanyName=dbe['Name']
        CompanyName=CompanyName.replace(" ","")
        try:
            logger = Logger()
            Company=spark.read.format("parquet").load(STAGE1_Configurator_Path+"/tblCompanyName")
            GLMap=spark.read.format("parquet").load(STAGE1_Configurator_Path+"/tblGLAccountMapping")
            SIH = spark.read.format("parquet").load(STAGE1_PATH+"/Sales Invoice Header")
            SCML = spark.read.format("parquet").load(STAGE1_PATH+"/Sales Cr_Memo Line")
            VE =spark.read.format("parquet").load(STAGE1_PATH+"/Value Entry")
            SCMH = spark.read.format("parquet").load(STAGE1_PATH+"/Sales Cr_Memo Header")
            DSE=spark.read.format("parquet").load(STAGE2_PATH+"/"+"Masters/DSE")
            COA=spark.read.format("parquet").load(STAGE2_PATH+"/"+"Masters/ChartofAccounts")
            PSOLD = spark.read.format("parquet").load(STAGE1_PATH+"/Posted Str Order Line Details")
            SIL =spark.read.format("parquet").load(STAGE1_PATH+"/Sales Invoice Line") 
            Company = Company.filter(col('DBName')==DBName).filter(col('NewCompanyName') == EntityName)
            Calendar_StartDate = Company.select('StartDate').rdd.flatMap(lambda x: x).collect()[0]
            Calendar_StartDate = datetime.datetime.strptime(Calendar_StartDate,"%Y-%m-%d").date()
            if datetime.date.today().month>int(MnSt)-1:
                 UIStartYr=datetime.date.today().year-int(yr)+1
            else:
                 UIStartYr=datetime.date.today().year-int(yr)
            UIStartDate=datetime.date(UIStartYr,int(MnSt),1)
            UIStartDate=max(Calendar_StartDate,UIStartDate)
            GLMap = GLMap.withColumnRenamed('GLRangeCategory','GLCategory')\
                             .withColumnRenamed('FromGLCode','FromGL')\
                             .withColumnRenamed('ToGLCode','ToGL')
            GLRange = GLMap.filter(GLMap["GLCategory"] == 'REVENUE').filter(GLMap["DBName"] == DBName)\
                                 .filter(GLMap["EntityName"] == EntityName).select("GLCategory","FromGL","ToGL")
            ServRange=GLMap.filter(GLMap["GLCategory"] == 'Service Revenue').filter(GLMap["DBName"] == DBName)\
                                 .filter(GLMap["EntityName"] == EntityName).select("GLCategory","FromGL","ToGL")
            SalesRange=GLMap.filter(GLMap["GLCategory"] == 'Sales Revenue').filter(GLMap["DBName"] == DBName)\
                                 .filter(GLMap["EntityName"] == EntityName).select("GLCategory","FromGL","ToGL")
            NoOfRows=GLRange.count()
            NoOfRows2=ServRange.count()
            NoOfRows3=SalesRange.count()
            SIH=SIH.select('No_','CurrencyFactor','PostingDate','SalespersonCode','Ship-toCity')
            SIH = SIH.filter(SIH['PostingDate']>=UIStartDate)
            SIL=SIL.select('DocumentNo_','Gen_Bus_PostingGroup','Gen_Prod_PostingGroup','LineNo_','Type','No_','Amount','ChargesToCustomer','PostingDate','DimensionSetID','Quantity')
            
            SIL = SIL.withColumn("GL_Link",concat_ws('|',SIL.Gen_Bus_PostingGroup.cast('string'), SIL.Gen_Prod_PostingGroup.cast('string')))\
                         .withColumn("LinkValueEntry",concat_ws('|',SIL.DocumentNo_.cast('string'),SIL.LineNo_.cast('string'),to_date(SIL.PostingDate).cast('string')))\
                         .withColumnRenamed('No_','Item_No').withColumnRenamed('PostingDate','SIL_PostingDate')
            SIL = SIL.filter(SIL['Type']!=4).filter(SIL['Quantity']!=0)
            SIL_DE = SIL
            SIL = SIL.filter(SIL['SIL_PostingDate']>=UIStartDate)
            SCMH = SCMH.select('No_','CurrencyFactor','PostingDate','SalespersonCode','Ship-toCity')
            SCMH = SCMH.filter(SCMH['PostingDate']>=UIStartDate)
            
            SCML = SCML.select('DocumentNo_','Gen_Bus_PostingGroup','Quantity','Gen_Prod_PostingGroup','LineNo_','Type','No_','Amount','DimensionSetID','PostingDate')
            SCML = SCML.withColumn("GL_Link",concat_ws('|',SCML.Gen_Bus_PostingGroup.cast('string'), SCML.Gen_Prod_PostingGroup.cast('string')))\
                         .withColumn("LinkValueEntry",concat_ws('|',SCML.DocumentNo_.cast('string'),SCML.LineNo_.cast('string'),to_date(SCML.PostingDate).cast('string')))\
                         .withColumnRenamed('No_','Item_No').withColumnRenamed('PostingDate','SCML_PostingDate')
            SCML = SCML.filter(SCML['Type']!=4).filter(SCML['Quantity']!=0)
            SCML_DE = SCML
            SCML = SCML.filter(SCML['SCML_PostingDate']>=UIStartDate)
            GPS = spark.read.format("parquet").load(STAGE1_PATH+"/General Posting Setup")
            GPS = GPS.select('Gen_Bus_PostingGroup','Gen_Prod_PostingGroup','SalesAccount','COGSAccount','SalesCreditMemoAccount')
                 
            GPS_Sales = GPS.withColumn("GL_Link",concat_ws('|',GPS.Gen_Bus_PostingGroup,GPS.Gen_Prod_PostingGroup))\
                         .withColumn("GLAccount",when(GPS.SalesAccount=='',0).otherwise(GPS.SalesAccount))
            GPS_Sales = GPS_Sales.select('GL_Link','GLAccount')
                 
            GPS_SCM = GPS.withColumn("GL_Link",concat_ws('|',GPS.Gen_Bus_PostingGroup,GPS.Gen_Prod_PostingGroup))\
                             .withColumn("GLAccount",when(GPS.SalesCreditMemoAccount=='',0).otherwise(GPS.SalesCreditMemoAccount))
            GPS_SCM = GPS_SCM.select('GL_Link','GLAccount')
            GPS_DE = GPS.withColumn("GL_Link",concat_ws('|',GPS.Gen_Bus_PostingGroup,GPS.Gen_Prod_PostingGroup))\
                             .withColumn("GLAccount",when(GPS.COGSAccount=='',0).otherwise(GPS.COGSAccount))
            GPS_DE = GPS_DE.select('GL_Link','GLAccount')
            VE = VE.select('DocumentNo_','DocumentLineNo_','PostingDate','InvoicedQuantity','EntryType','SourceCode','DimensionSetID','CostAmountActual','ItemLedgerEntryType','ItemNo_')
            VE = VE.filter(VE['PostingDate']>=UIStartDate)
            GL =spark.read.format("parquet").load(STAGE1_PATH+"/G_L Entry")
             
            GL = GL.filter(GL['PostingDate']>=UIStartDate)
            SI = SIL.join(SIH, SIL['DocumentNo_']==SIH['No_'], 'left')
            SI = SI.join(GPS_Sales,'GL_Link','left')
            SI = SI.withColumn('GLAccount',when(SI['Type']==1,SI['Item_No']).otherwise(SI['GLAccount']))
            SI = SI.withColumn('TransactionType',lit('Sales'))
                 
            SCM = SCML.join(SCMH, SCML['DocumentNo_']==SCMH['No_'], 'left')
            SCM = SCM.join(GPS_SCM,'GL_Link','left')
            SCM = SCM.withColumn('GLAccount',when(SCM['Type']==1,SCM['Item_No']).otherwise(SCM['GLAccount']))
            SCM = SCM.withColumn('Amount',SCM['Amount']*(-1))
            SCM = SCM.withColumn('TransactionType',lit('SalesCreditMemo'))
            Sales = SCM.unionByName(SI, allowMissingColumns = True)
             
            vSalesRange='1=1'
            for i in range(0,NoOfRows3):
                if i==0:
                    vSalesRange = ((Sales.GLAccount>=SalesRange.select('FromGL').collect()[0]['FromGL']) \
                             & (Sales.GLAccount<=SalesRange.select('ToGL').collect()[0]['ToGL']))
                else:
                    vSalesRange = (vSalesRange) | ((Sales.GLAccount>=SalesRange.select('FromGL').collect()[i]['FromGL']) \
                                                 & (Sales.GLAccount<=SalesRange.select('ToGL').collect()[i]['ToGL']))
         
            vServRange='1=1'
            for i in range(0,NoOfRows2):
                if i==0:
                    vServRange = ((Sales.GLAccount>=ServRange.select('FromGL').collect()[0]['FromGL']) \
                             & (Sales.GLAccount<=ServRange.select('ToGL').collect()[0]['ToGL']))
                else:
                    vServRange = (vServRange) | ((Sales.GLAccount>=ServRange.select('FromGL').collect()[i]['FromGL']) \
                                                 & (Sales.GLAccount<=ServRange.select('ToGL').collect()[i]['ToGL']))
        
             
            for i in range(0,NoOfRows):
                    if i==0:
                        Range = (Sales.GLAccount>=GLRange.select('FromGL').collect()[0]['FromGL']) \
                            & (Sales.GLAccount<=GLRange.select('ToGL').collect()[0]['ToGL'])
             
                    else:
                        Range = (Range) | ((Sales.GLAccount>=GLRange.select('FromGL').collect()[i]['FromGL']) \
                                            & (Sales.GLAccount<=GLRange.select('ToGL').collect()[i]['ToGL']))
             
            Sales = Sales.filter(((Sales.Type!=2) | ((Sales.Type==2) & (Range))) & ((Sales.Type!=1) | ((Sales.Type==1) & (Range))))
             
             
            Sales = Sales.withColumn('ChargesToCustomer',Sales['ChargesToCustomer'].cast('decimal'))
            Sales = Sales.na.fill({'ChargesToCustomer':0})
            Sales = Sales.withColumn('Amount',when(Sales.CurrencyFactor==0,when(Sales.ChargesToCustomer==0,Sales.Amount).otherwise(Sales.Amount+Sales.ChargesToCustomer))\
                             .otherwise(when(Sales.ChargesToCustomer==0,Sales.Amount/Sales.CurrencyFactor).otherwise((Sales.Amount+Sales.ChargesToCustomer)/(Sales.CurrencyFactor))))
            Sales = Sales.withColumn('RevenueType',when((vSalesRange), lit('Sales')).when((vServRange), lit('Service')).otherwise(lit('Other')))
            Sales = Sales.select('GL_Link','SalespersonCode','Ship-toCity','TransactionType','LinkValueEntry','DocumentNo_','DimensionSetID','PostingDate','Item_No','Amount','Quantity','GLAccount','RevenueType')
            VE = VE.withColumn("LinkValueEntry",concat_ws("|",VE.DocumentNo_,VE.DocumentLineNo_,to_date(VE.PostingDate).cast('string')))
            VE = VE.withColumn('CostAmountActual',VE['CostAmountActual']*(-1)) 
            ValueEntry = VE
            VE = VE.groupBy('LinkValueEntry').agg({'CostAmountActual':'sum'})\
                         .withColumnRenamed('sum(CostAmountActual)','CostAmountActual')
                 
            Sales = Sales.join(VE,'LinkValueEntry','left')
            Sales = Sales.withColumn('SystemEntry',lit(1))
            Sales.cache() 
            print(Sales.count())
            Documents = Sales.select('DocumentNo_').distinct()
            Documents = Documents.withColumn('SysDocFlag',lit(1))
            ValueEntry = ValueEntry.join(Documents,'DocumentNo_','left')
            SysEntries = Sales.select('LinkValueEntry').distinct()
            SysEntries = SysEntries.withColumn('SysValueEntryFlag',lit(1))
            ValueEntry = ValueEntry.join(SysEntries,'LinkValueEntry','left')
            ValueEntry = ValueEntry.filter(((ValueEntry.SysDocFlag==1)&(ValueEntry.SysValueEntryFlag.isNull()))|((ValueEntry.SysDocFlag.isNull())&(ValueEntry.ItemLedgerEntryType==1)))
            ValueEntry = ValueEntry.withColumn('TransactionType',lit('Revaluation Entries'))
            
            ValueEntry = ValueEntry.join(DSE,"DimensionSetID",'left')
            finalDF = Sales.join(DSE,"DimensionSetID",'left')
            ValueEntry=ValueEntry.drop('DimensionSetID','LinkValueEntry','DocumentLineNo_','ItemLedgerEntryType','SysDocFlag','SysValueEntryFlag')
            finalDF = finalDF.drop('DimensionSetID','LinkValueEntry')
            finalDF = finalDF.unionByName(ValueEntry, allowMissingColumns = True)
            finalDF = finalDF.withColumn('PostingDate',to_date(finalDF['PostingDate']))
            finalDF = finalDF.withColumn('LinkItemKey',concat(finalDF["DBName"],lit('|'),finalDF["EntityName"],lit('|'),finalDF["Item_No"]))\
                         .withColumn('LinkDateKey',concat(finalDF["DBName"],lit('|'),finalDF["EntityName"],lit('|'),finalDF["PostingDate"]))
            finalDF.cache()
            print(finalDF.count())
            DocumentNos = finalDF.withColumn('link',concat_ws('|',finalDF['DocumentNo_'],finalDF['GLAccount']))
            DocumentNos = DocumentNos.select('link','SystemEntry').distinct()   
            SIL_DE = SIL_DE.select('DocumentNo_','GL_Link').distinct()
            SCML_DE = SCML_DE.select('DocumentNo_','GL_Link').distinct()
            ManualCOGSDocuments = SIL_DE.unionByName(SCML_DE)
            ManualCOGSDocuments = ManualCOGSDocuments.join(GPS_DE,'GL_Link','left')
            ManualCOGSDocuments = ManualCOGSDocuments.withColumn('link',
                                 concat_ws('|',ManualCOGSDocuments['DocumentNo_'],ManualCOGSDocuments['GLAccount']))\
                                                     .withColumn('SystemEntry',lit(1))
            ManualCOGSDocuments = ManualCOGSDocuments.select('link','SystemEntry').distinct()
            DocumentNos = DocumentNos.unionByName(ManualCOGSDocuments)
            GL=GL.select('Amount','G_LAccountNo_','PostingDate','SourceCode','DocumentNo_','DimensionSetID')
            GL = GL.filter(GL['SourceCode']!='CLSINCOME')\
                     .withColumn('link',concat_ws('|',GL['DocumentNo_'],GL['G_LAccountNo_']))
            GL = GL.withColumnRenamed('G_LAccountNo_','GLAccount')
            GL = GL.join(DocumentNos,'link','left')
            GL = GL.na.fill({'SystemEntry':0})
            GL = GL.filter(GL['SystemEntry']==0)
            COA = COA.select('GLAccount','GLRangeCategory')
            GL = GL.join(COA,'GLAccount','left')
            GL=GL.withColumn("PostingDate",to_date(col("PostingDate")))
            GL=GL.withColumn("DBName",concat(lit(DBName))).withColumn("EntityName",concat(lit(EntityName)))
            GL =GL.withColumn('LinkDateKey',concat(GL["DBName"],lit('|'),GL["EntityName"],lit('|'),GL["PostingDate"]))
            GL.cache()
            print(GL.count())
            GL=GL.select('GLRangeCategory','DocumentNo_','Amount','LinkDateKey','DimensionSetID','PostingDate','DBName','EntityName','GLAccount')
            GLEntry_Sales = GL.filter(GL['GLRangeCategory'].isin(['REVENUE']))
             
            GLEntry_DE = GL.filter(GL['GLRangeCategory'].isin(['DE']))
            AggCOGS = GLEntry_DE.groupBy('DocumentNo_').agg({'Amount':'sum'})\
                                 .withColumnRenamed('sum(Amount)','Cost_Amount')
            AggCOGS = AggCOGS.filter(AggCOGS['Cost_Amount']!=0)
            GLEntry_DE = AggCOGS.join(GLEntry_DE,'DocumentNo_','left')
            GLEntry_Sales = GLEntry_Sales.withColumn('Amount',GLEntry_Sales['Amount']*(-1))
            GLEntry_DE = GLEntry_DE.withColumnRenamed('Amount','CostAmountActual')
            GLEntry_Sales=GLEntry_Sales.select('Amount','LinkDateKey','DimensionSetID','PostingDate','GLAccount')
            GLEntry_DE=GLEntry_DE.select('CostAmountActual','LinkDateKey','PostingDate','DimensionSetID','GLAccount')
            GLEntry_Sales = GLEntry_Sales.join(DSE,"DimensionSetID",'left')  
            GLEntry_Sales.cache()
            print(GLEntry_Sales.count())
            GLEntry_Sales.coalesce(1).write.format("parquet").mode("overwrite").option("overwriteSchema", "true").save(STAGE2_PATH+"/"+"Sales/SalesGLEntry")
            GLEntry_DE = GLEntry_DE.join(DSE,"DimensionSetID",'left')
            GLEntry_DE.cache()
            print(GLEntry_DE.count())
            GLEntry_DE.coalesce(1).write.format("parquet").mode("overwrite").option("overwriteSchema", "true").save(STAGE2_PATH+"/"+"Sales/ManualCOGS")   
            finalDF.cache()
            print(finalDF.count())
            finalDF=finalDF.drop('SystemEntry','Item_No')
            finalDF.coalesce(1).write.format("parquet").mode("overwrite").option("overwriteSchema", "true").save(STAGE2_PATH+"/"+"Sales/Sales")

            logger.endExecution()
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"
            
            log_dict = logger.getSuccessLoggedRecord("Sales.Sales", DBName, EntityName, finalDF.count(), len(finalDF.columns), IDEorBatch)
            log_df = spark.createDataFrame(log_dict, logger.getSchema())
            log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)
        except Exception as ex:
            exc_type,exc_value,exc_traceback=sys.exc_info()
            print("Error:",ex)
            print("type - "+str(exc_type))
            print("File - "+exc_traceback.tb_frame.f_code.co_filename)
            print("Error Line No. - "+str(exc_traceback.tb_lineno))
            ex = str(ex)
            logger.endExecution()
        
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"
            os.system("spark-submit "+Kockpit_Path+"/Email.py 1 Sales '"+CompanyName+"' "+DBEntity+" "+str(exc_traceback.tb_lineno)+"")
            log_dict = logger.getErrorLoggedRecord('Sales.Sales', DBName, EntityName, str(ex), str(exc_traceback.tb_lineno), IDEorBatch)
            log_df = spark.createDataFrame(log_dict, logger.getSchema())
            log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)
print('Sales_Sales completed: ' + str((dt.datetime.now()-st).total_seconds()))

          
        
        