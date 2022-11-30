from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import io,os,datetime
from datetime import date
from pyspark.sql.functions import col,concat,concat_ws,year,month,to_date,lit,round as col_round,split,round
import time,sys
from pyspark.sql.types import *
from builtins import str
from os.path import dirname, join, abspath
from distutils.command.check import check
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
FilePathSplit = Filepath.split('\\')
DBName = FilePathSplit[-5]
EntityName = FilePathSplit[-4]
DBEntity = DBName+EntityName
entityLocation = DBName+EntityName
Datelog = datetime.datetime.now().strftime('%Y-%m-%d')
STAGE1_Configurator_Path=Kockpit_Path+"/" +DBName+"/" +EntityName+"/" +"Stage1/ConfiguratorData/"
STAGE1_PATH=Kockpit_Path+"/" +DBName+"/" +EntityName+"/" +"Stage1/ParquetData"
STAGE2_PATH=Kockpit_Path+"/" +DBName+"/" +EntityName+"/" +"Stage2/ParquetData"
sqlCtx, spark = getSparkConfig("local[*]", "Stage2:ProfitLoss")
def finance_ProfitLoss():
    
    for dbe in config["DbEntities"]:
        if dbe['ActiveInactive']=='true' and  dbe['Location']==DBEntity:
            CompanyName=dbe['Name']
            CompanyName=CompanyName.replace(" ","")
            try:
                logger = Logger()
                GL_Entry_Table =spark.read.format("parquet").load(STAGE1_PATH+"/G_L Entry")
                GL_Account_Table =spark.read.format("parquet").load(STAGE1_PATH+"/G_L Account")
                Company =spark.read.format("parquet").load(STAGE1_Configurator_Path+"/tblCompanyName")
                COA = spark.read.format("parquet").load(STAGE1_PATH+"/"+"../../"+"Stage2/ParquetData/Masters/ChartofAccounts")
                Company = Company.filter(col('DBName')==DBName).filter(col('NewCompanyName') == EntityName)
                Calendar_StartDate = Company.select('StartDate').rdd.flatMap(lambda x: x).collect()[0]
                Calendar_StartDate = datetime.datetime.strptime(Calendar_StartDate,"%Y-%m-%d").date()
                if datetime.date.today().month>int(MnSt)-1:
                    UIStartYr=datetime.date.today().year-int(yr)+1
                else:
                    UIStartYr=datetime.date.today().year-int(yr)
                UIStartDate=datetime.date(UIStartYr,int(MnSt),1)
                UIStartDate=max(Calendar_StartDate,UIStartDate)
               
                GL_Account_Table = GL_Account_Table.select("No_","Income_Balance")\
                                            .withColumnRenamed('No_','No').drop('DBName','EntityName')
                GL_Entry_Table = GL_Entry_Table.withColumn('PostingDate',to_date(GL_Entry_Table['PostingDate'])).drop('DBName','EntityName')
                GLEntry = GL_Entry_Table.join(GL_Account_Table,GL_Entry_Table['G_LAccountNo_']==GL_Account_Table['No'],'left')
                GLEntry = GLEntry.filter(GLEntry['Income_Balance']==0)\
                            .filter(GLEntry["SourceCode"]!='CLSINCOME')\
                            .filter(year(GLEntry["PostingDate"])!='1753')\
                            .filter(GLEntry["PostingDate"]>=UIStartDate)
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
                COA = COA.select('GLAccount','GLRangeCategory')
                GLEntry = GLEntry.join(COA,'GLAccount','left')
                GLEntry = GLEntry.withColumn("LinkDateKey",concat_ws("|",lit(DBName),lit(EntityName),GLEntry.LinkDate))\
                                .withColumn("Link_GLAccount_Key",concat_ws("|",lit(DBName),lit(EntityName),GLEntry.GLAccount))\
                                .withColumn("DBName",lit(DBName))\
                                .withColumn("EntityName",lit(EntityName))         
                GLEntry = GLEntry.withColumn('Amount',round('Amount',5))\
                                .withColumn('Cost_Amount',round('Cost_Amount',5))\
                                .withColumn('CreditAmount',round('CreditAmount',5))\
                                .withColumn('DebitAmount',round('DebitAmount',5))
                DSE = spark.read.format("parquet").load(STAGE1_PATH+"/"+"../../"+"Stage2/ParquetData/Masters/DSE").drop("DBName","EntityName")
                GLEntry =  GLEntry.join(DSE,"DimensionSetID",'left')
                GLEntry=GLEntry.drop("GlobalDimension1Code","GlobalDimension12Code","GlobalDimension1Code13","GlobalDimension14Code","GlobalDimension1Code2")
                GLEntry.cache()
                print(GLEntry.count())
                GLEntry.coalesce(1).write.format("parquet").mode("overwrite").option("overwriteSchema", "true").save(STAGE2_PATH+"/"+"Finance/ProfitLoss")
                
                logger.endExecution()
                try:
                    IDEorBatch = sys.argv[1]
                except Exception as e :
                    IDEorBatch = "IDLE"
            
                log_dict = logger.getSuccessLoggedRecord("Finance.ProfitLoss", DBName, EntityName, GLEntry.count(), len(GLEntry.columns), IDEorBatch)
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
                os.system("spark-submit "+Kockpit_Path+"/Email.py 1 ProfitLoss '"+CompanyName+"' "+DBEntity+" "+str(exc_traceback.tb_lineno)+" ")
        
                log_dict = logger.getErrorLoggedRecord('Finance.ProfitLoss', DBName, EntityName, str(ex), str(exc_traceback.tb_lineno), IDEorBatch)
                log_df = spark.createDataFrame(log_dict, logger.getSchema())
                log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)
    print('finance_ProfitLoss completed: ' + str((dt.datetime.now()-st).total_seconds()))     

if __name__ == '__main__':
    
    finance_ProfitLoss()  