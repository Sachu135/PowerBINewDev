from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import  col
import datetime, time
import datetime as dt
import os,sys
from os.path import dirname, join, abspath
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
STAGE1_PATH=Kockpit_Path+"/" +DBName+"/" +EntityName+"/" +"Stage1/ParquetData"
STAGE2_PATH=Kockpit_Path+"/" +DBName+"/" +EntityName+"/" +"Stage2/ParquetData"
sqlCtx, spark = getSparkConfig("local[*]", "Stage2:Collection")
def finance_Collection():
    
    cdate = datetime.datetime.now().strftime('%Y-%m-%d')
    for dbe in config["DbEntities"]:
        if dbe['ActiveInactive']=='true' and  dbe['Location']==DBEntity:
            CompanyName=dbe['Name']
            CompanyName=CompanyName.replace(" ","")
            try:
                logger = Logger()
                dcleDF =spark.read.format("parquet").load(STAGE1_PATH+"/Detailed Cust_ Ledg_ Entry").where((col("EntryType") == 1) & (col("DocumentType") == 1))
                cleDF = spark.read.format("parquet").load(STAGE1_PATH+"/Cust_ Ledger Entry")
                cond = [dcleDF["Cust_LedgerEntryNo_"] == cleDF["EntryNo_"]]
                finalDF = dcleDF.join(cleDF, cond, 'left')
                finalDF = RenameDuplicateColumns(finalDF)
                finalDF.write.option("maxRecordsPerFile", 10000).format("parquet").mode("overwrite").option("overwriteSchema", "true").save(STAGE2_PATH+"/"+"Finance/Collection")
                logger.endExecution()
                try:
                    IDEorBatch = sys.argv[1]
                except Exception as e :
                    IDEorBatch = "IDLE"
            
                log_dict = logger.getSuccessLoggedRecord("Finance.Collection", DBName, EntityName, finalDF.count(), len(finalDF.columns), IDEorBatch)
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
                os.system("spark-submit "+Kockpit_Path+"/Email.py 1 Collection '"+CompanyName+"' "+DBEntity+" "+str(exc_traceback.tb_lineno)+" ")
        
                log_dict = logger.getErrorLoggedRecord('Finance.Collection', DBName, EntityName, str(ex), str(exc_traceback.tb_lineno), IDEorBatch)
                log_df = spark.createDataFrame(log_dict, logger.getSchema())
                log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)
    print('finance_Collection completed: ' + str((dt.datetime.now()-st).total_seconds()))     

if __name__ == '__main__':
    
    finance_Collection()  