
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import datetime,time,sys  
from datetime import date
from pyspark.sql.functions import col,max as max_,concat,concat_ws,year,when,month,to_date,lit,sum,count,round,split,last_day             
from pyspark.sql.types import *
from builtins import str
import traceback
import os
from os.path import dirname, join, abspath
import datetime as dt
from datetime import datetime
from distutils.command.check import check
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
sqlCtx,spark=getSparkConfig(SPARK_MASTER, "Stage2:Finance-BalanceSheet")
def finance_BalanceSheet():
    cdate = datetime.datetime.now().strftime('%Y-%m-%d')
    for dbe in config["DbEntities"]:
        if dbe['ActiveInactive']=='true' and  dbe['Location']==DBEntity:
            CompanyName=dbe['Name']
            CompanyName=CompanyName.replace(" ","")
            try:
                logger = Logger()
                VLE =spark.read.format("delta").load(STAGE1_PATH+"/Vendor Ledger Entry")
                CLE =spark.read.format("delta").load(STAGE1_PATH+"/Cust_ Ledger Entry")
                Company= spark.read.format("delta").load(STAGE1_Configurator_Path+"/tblCompanyName")
                GL_Entry_Table= spark.read.format("delta").load(STAGE1_PATH+"/G_L Entry")
                GL_Account_Table = spark.read.format("delta").load(STAGE1_PATH+"/G_L Account")  
                VLE = VLE.select('VendorNo_','DocumentNo_')
                CLE = CLE.select('CustomerNo_','DocumentNo_')     
                Company = Company.filter(col('DBName')==DBName).filter(col('NewCompanyName') == EntityName)
                Company = Company.select("StartDate","EndDate")
                Calendar_StartDate = Company.select('StartDate').rdd.flatMap(lambda x: x).collect()[0]
                Calendar_StartDate = datetime.datetime.strptime(Calendar_StartDate,"%Y-%m-%d").date()
                
                if datetime.date.today().month>int(MnSt)-1:
                    UIStartYr=datetime.date.today().year-int(yr)+1
                else:
                    UIStartYr=datetime.date.today().year-int(yr)
                UIStartDate=datetime.date(UIStartYr,int(MnSt),1)
                UIStartDate=max(Calendar_StartDate,UIStartDate)
                
                Calendar_EndDate_conf=Company.select(Company.EndDate).collect()[0]["EndDate"]
                Calendar_EndDate_conf = Calendar_EndDate_conf.split("-")
                Calendar_EndDate_conf = datetime.date(int(Calendar_EndDate_conf[0]),int(Calendar_EndDate_conf[1]),int(Calendar_EndDate_conf[2]))
               
                cdate = cdate.split("-")
                cdate = datetime.date(int(cdate[0]),int(cdate[1]),int(cdate[2]))
                Calendar_EndDate=min(Calendar_EndDate_conf,cdate)
                days = (Calendar_EndDate-UIStartDate).days
                joined  = [GL_Entry_Table['G_LAccountNo_']==GL_Account_Table['No_']]
                joined =  GL_Entry_Table.join(GL_Account_Table, joined ,'left')
                GLAcc_Entry1 = joined.filter((col('SourceCode')=='CLSINCOME') & (year(joined.PostingDate)!=1753) & (col('Income_Balance')==1))
                GLAcc_Entry1 = GLAcc_Entry1.select('G_LAccountNo_','PostingDate').withColumnRenamed('G_LAccountNo_','GLAccount')
            
                if GLAcc_Entry1.limit(1).count()==0:
            
                    GLAcc_Entry2 = joined.filter(col('Income_Balance')==1).filter(year(joined.PostingDate)!=1753)\
                                            .select('G_LAccountNo_','PostingDate')
            
                    Max_GL = max([int(i.G_LAccountNo_) for i in GLAcc_Entry2.select('G_LAccountNo_').distinct().collect()])*100
                    GL_Date = Kockpit.add_months(Calendar_StartDate)
                    global spark_schema
                    spark_schema = StructType([
                                StructField('GLAccount',StringType(),True),
                                StructField('PostingDate',DateType(),True)])
            
                    gl_dict = [{'GLAccount':Max_GL,'PostingDate':GL_Date}]
                    vGlCode = spark.createDataFrame(gl_dict,spark_schema)
                    GLAcc_Entry1 = GLAcc_Entry1.unionAll(vGlCode)
                GLAcc_Entry1 = GLAcc_Entry1.withColumn('LinkDate',col('PostingDate'))
                vGLLower=GLAcc_Entry1.select(GLAcc_Entry1.GLAccount).collect()[0]["GLAccount"]
                vGLUpper=GLAcc_Entry1.select(GLAcc_Entry1.GLAccount).collect()[GLAcc_Entry1.count()-1]["GLAccount"]
                GLAcc_Entry1 = GLAcc_Entry1.groupBy('GLAccount').agg({'LinkDate':'max','PostingDate':'min'})\
                                    .withColumnRenamed('max(LinkDate)','MaxPDate')\
                                    .withColumnRenamed('min(PostingDate)','MinPDate')
                GLAcc_Entry1 = GLAcc_Entry1.select('GLAccount','MinPDate','MaxPDate')
                GLAcc_Entry1 = GLAcc_Entry1.withColumn('MinPDate',to_date(GLAcc_Entry1['MinPDate']))\
                                        .withColumn('MaxPDate',to_date(GLAcc_Entry1['MaxPDate']))
                
                vTableTemp1 = GLAcc_Entry1.select('MinPDate')
                vTableTemp1 = vTableTemp1.withColumnRenamed('MinPDate','MaxPDate')\
                                    .withColumn('GLAccount',lit(vGLLower))\
                                    .withColumn('MinPDate',lit(Calendar_StartDate.replace(month=3, day=31, year=Calendar_StartDate.year-1)))
                vTableTemp1 = vTableTemp1.select('GLAccount','MinPDate','MaxPDate')
                vTableTemp1 = vTableTemp1.withColumn('MinPDate',to_date(vTableTemp1['MinPDate']))\
                                        .withColumn('MaxPDate',to_date(vTableTemp1['MaxPDate']))
            
                vTableTemp2 = GLAcc_Entry1.select('MaxPDate')
                vTableTemp2 = vTableTemp2.withColumnRenamed('MaxPDate','MinPDate')\
                                        .withColumn('GLAccount',lit(vGLLower))\
                                        .withColumn('MaxPDate',lit(datetime.datetime.today().replace(month=3, day=31, year=datetime.datetime.today().year+1)))
                vTableTemp2 = vTableTemp2.select('GLAccount','MinPDate','MaxPDate')
                vTableTemp2 = vTableTemp2.withColumn('MinPDate',to_date(vTableTemp2['MinPDate']))\
                                        .withColumn('MaxPDate',to_date(vTableTemp2['MaxPDate']))
                vTableTemp2 = vTableTemp2.unionAll(vTableTemp1).unionAll(GLAcc_Entry1)
                
               
                GLEntry1=[]
                for i in range(0,vTableTemp2.count()):
                    GL=vTableTemp2.select(vTableTemp2.GLAccount).collect()[i]["GLAccount"]
                    MINDATE=vTableTemp2.select(vTableTemp2.MinPDate).collect()[i]["MinPDate"]
                    MAXDATE=vTableTemp2.select(vTableTemp2.MaxPDate).collect()[i]["MaxPDate"]
                    j=-1;
                    while(MINDATE.replace(year=MINDATE.year+(j+1))<MAXDATE):
                        j=j+1;
                        GLEntry1.append({'GLAccount':GL,'MaxPDate':MINDATE.replace(year=MINDATE.year+(j+1))\
                                     ,'MinPDate':MINDATE.replace(year=MINDATE.year+j,day=1,month=4)})
            
                GLEntry1=spark.createDataFrame(GLEntry1).distinct()
                
                joined2 = joined.withColumn('PostingDate',to_date(joined['PostingDate']))
            
                GL_Acc_Entry3 = joined2.filter((year(joined2['PostingDate'])!=1753))\
                                    .filter(joined2['PostingDate']<=Calendar_EndDate)\
                        .filter(joined2['SourceCode']!='CLSINCOME')\
                        .filter(joined2['Income_Balance']==0)
            
                GL_Acc_Entry3 = GL_Acc_Entry3.withColumn('PostingYear',when(month(col('PostingDate'))<MnSt,year(col('PostingDate'))-1)\
                                                                .otherwise(year(col('PostingDate'))))\
                                            .withColumn('PostingMonth',when(month(col('PostingDate'))<MnSt,month(col('PostingDate'))+(13-MnSt))\
                                                                .otherwise(month(col('PostingDate'))-MnSt+1))\
                                            .withColumnRenamed('EntryNo_','Entry_No')\
                                            .withColumnRenamed('DocumentNo_','Document_No')\
                                            .withColumnRenamed('Description','GL_Description')\
                                            .withColumnRenamed('EntryNo_','Entry_No')\
                                            .withColumn('LinkDate',GL_Acc_Entry3['PostingDate'])
                GL_Acc_Entry3 = GL_Acc_Entry3.select('Entry_No','PostingDate','PostingYear','PostingMonth'
                                                   ,'LinkDate','Document_No','GL_Description','Amount'
                                                   ,'SourceCode','DebitAmount','CreditAmount','Income_Balance')
                list=GL_Acc_Entry3.select('PostingDate').distinct().collect()
                NoOfRows=len(list)
                data1=[]
                GLEntry2 = GLEntry1.collect()
                NoOfBuckets=len(GLEntry2)
                for i in range(0,NoOfRows):
                    n=list[i].PostingDate
                    for j in range(0, NoOfBuckets):
                        if GLEntry2[j].MinPDate <= n <= GLEntry2[j].MaxPDate:
                            data1.append({'From_Date':GLEntry2[j].MinPDate,'To_Date':GLEntry2[j].MaxPDate,'Posting_Date1':n})
                            break
                GLEntry2=spark.createDataFrame(data1).distinct()
                cond = [GL_Acc_Entry3.PostingDate == GLEntry2.Posting_Date1]
                GLEntry=GL_Acc_Entry3.join(GLEntry2,cond,'left').drop('Posting_Date1')
                cond1 = [(GLEntry.From_Date == GLEntry1.MinPDate) & (GLEntry.To_Date == GLEntry1.MaxPDate)]
                GLEntry_Temp=GLEntry.join(GLEntry1,cond1,'left').drop('From_Date').drop('To_Date').drop('MinPDate').drop('MaxPDate')
                GL_Acc_Entry2 = joined2.filter((year(joined2.PostingDate)!=1753) & (joined2.SourceCode!='CLSINCOME')\
                                          & (joined2.PostingDate<=Calendar_EndDate) & (col('Income_Balance')==1))
                GL_Acc_Entry2 = GL_Acc_Entry2.withColumn('PostingYear',when(month(col('PostingDate'))<MnSt,year(col('PostingDate'))-1)\
                                                                .otherwise(year(col('PostingDate'))))\
                                            .withColumn('PostingMonth',when(month(col('PostingDate'))<MnSt,month(col('PostingDate'))+(13-MnSt))\
                                                                .otherwise(month(col('PostingDate'))-MnSt+1))\
                                            .withColumnRenamed('EntryNo_','Entry_No')\
                                            .withColumnRenamed('DocumentNo_','Document_No')\
                                            .withColumnRenamed('Description','GL_Description')\
                                            .withColumnRenamed('EntryNo_','Entry_No')\
                                            .withColumn('LinkDate',GL_Acc_Entry2['PostingDate'])\
                                            .withColumnRenamed('G_LAccountNo_','GLAccount')
                GL_Acc_Entry2 = GL_Acc_Entry2.select('Entry_No','PostingDate','PostingYear','PostingMonth'
                                                   ,'LinkDate','Document_No','GL_Description','Amount'
                                                   ,'SourceCode','DebitAmount','CreditAmount','Income_Balance','GLAccount')
                GLEntry_Temp= CONCATENATE(GL_Acc_Entry2,GLEntry_Temp, spark)
                GL_Two = GLEntry_Temp.withColumn('Year_X',year('PostingDate'))
                GL_Two = GL_Two.withColumn('Month_X',month('PostingDate'))
                GL_Two = GL_Two.withColumn('YMX',concat_ws('_','Year_X','Month_X'))
                GL_Two = GL_Two.withColumn('Key',concat_ws('_','Document_No','YMX'))
                GL_Two_Cust = GL_Two.select('Document_No','YMX','Amount')
                cond = [GL_Two.Document_No == CLE.DocumentNo_]
                GL_Two_Cust = GL_Two_Cust.join(CLE , cond, how = 'left')
                GL_Two_Cust = GL_Two_Cust.groupBy('CustomerNo_','YMX').agg({'Amount':'sum'}).withColumnRenamed('sum(Amount)','Amount')
                GL_Two_Cust = GL_Two_Cust.withColumn('Flag_Cust',when(GL_Two_Cust['Amount']<0,'1'))
                GL_Two_Cust = GL_Two_Cust.filter(GL_Two_Cust['Flag_Cust'] == '1')
                GL_Two_Cust = GL_Two_Cust.join(CLE,'CustomerNo_', how = 'left')
                GL_Two_Cust = GL_Two_Cust.withColumn('Key',concat_ws('_','DocumentNo_','YMX'))
                GL_Two_Cust = GL_Two_Cust.where(col("DocumentNo_").isNotNull())
                GL_Two_Cust = GL_Two_Cust.select('Key','Flag_Cust')
                GL_Two = GL_Two.join(GL_Two_Cust,'Key',how = 'left')
                GL_Two_Ven = GL_Two.select('Document_No','YMX','Amount')
                cond = [GL_Two.Document_No == VLE.DocumentNo_]
                GL_Two_Ven = GL_Two_Ven.join(VLE , cond, how = 'left')
               
                GL_Two_Ven = GL_Two_Ven.groupBy('VendorNo_','YMX').agg({'Amount':'sum'}).withColumnRenamed('sum(Amount)','Amount')
                GL_Two_Ven = GL_Two_Ven.withColumn('Flag_Ven',when(GL_Two_Ven['Amount']<0,'1'))
                GL_Two_Ven = GL_Two_Ven.filter(GL_Two_Ven['Flag_Ven'] == '1')
                GL_Two_Ven = GL_Two_Ven.join(VLE,'VendorNo_', how = 'left')
              
                GL_Two_Ven = GL_Two_Ven.withColumn('Key',concat_ws('_','DocumentNo_','YMX'))
                GL_Two_Ven = GL_Two_Ven.where(col("DocumentNo_").isNotNull())
                GL_Two_Ven = GL_Two_Ven.select('Key','Flag_Ven')
                GL_Two = GL_Two.join(GL_Two_Ven,'Key',how = 'left')
                GLEntry_Temp = GL_Two.select('PostingDate','Amount','SourceCode','GLAccount')
                GLEntry_Temp=GLEntry_Temp.groupBy('PostingDate','GLAccount').agg({'Amount':'sum'}).withColumnRenamed('sum(Amount)','Amount')
                data =[]
                for single_date in Kockpit.daterange(UIStartDate, Calendar_EndDate):
                    data.append({'Link_date':single_date})
            
                schema = StructType([
                    StructField("Link_date", DateType(),True)
                ])
                records=spark.createDataFrame(data,schema)
            
                records=records.select(last_day(records.Link_date).alias('Link_date')).distinct().sort('Link_date')
            
                records=records.withColumn("Link_date", \
                              when(records["Link_date"] == Kockpit.last_day_of_month(cdate), cdate).otherwise(records["Link_date"]))
            
                spark.conf.set("spark.sql.crossJoin.enabled", 'true')
                GLEntry_Temp = GLEntry_Temp.join(records).select('PostingDate','Amount','GLAccount','Link_date')
                GLEntry_Temp=GLEntry_Temp.filter(GLEntry_Temp['Link_date']>= GLEntry_Temp['PostingDate'])
                GLEntry_Temp=GLEntry_Temp.groupBy('Link_date','GLAccount').agg({'Amount':'sum'}).withColumnRenamed('sum(Amount)','Amount')   
                GLEntry_Temp=GLEntry_Temp.withColumn("DBName",concat(lit(DBName))).withColumn("EntityName",concat(lit(EntityName)))
                GLEntry_Temp = GLEntry_Temp.withColumn('DBName',lit(DBName))\
                                        .withColumn('EntityName',lit(EntityName))\
                                        .withColumnRenamed('Link_date','LinkDate')
                GLEntry_Temp = GLEntry_Temp.withColumn('LinkGLAccountKey',concat_ws('|',GLEntry_Temp['DBName'],GLEntry_Temp['EntityName'],GLEntry_Temp['GLAccount']))\
                                        .withColumn('LinkDateKey',concat_ws('|',GLEntry_Temp['DBName'],GLEntry_Temp['EntityName'],GLEntry_Temp['LinkDate']))
                GLEntry_Temp = GLEntry_Temp.withColumn('Amount',round('Amount',5))
                GLEntry_Temp.cache()
                print(GLEntry_Temp.count())
                GLEntry_Temp.coalesce(1).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(STAGE2_PATH+"/"+"Finance/BalanceSheet")
                
                logger.endExecution()
                    
                try:
                    IDEorBatch = sys.argv[1]
                except Exception as e :
                    IDEorBatch = "IDLE"
            
                log_dict = logger.getSuccessLoggedRecord("Finance.BalanceSheet", DBName, EntityName, GLEntry_Temp.count(), len(GLEntry_Temp.columns), IDEorBatch)
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
                os.system("spark-submit "+Kockpit_Path+"/Email.py 1 BalanceSheet '"+CompanyName+"' "+DBEntity+" "+str(exc_traceback.tb_lineno)+" ")
                
                log_dict = logger.getErrorLoggedRecord('Finance.BalanceSheet', DBName, EntityName, str(ex), str(exc_traceback.tb_lineno), IDEorBatch)
                log_df = spark.createDataFrame(log_dict, logger.getSchema())
                log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)
    print('finance_BalanceSheet completed: ' + str((dt.datetime.now()-st).total_seconds()))
if __name__ == "__main__":
    finance_BalanceSheet()
