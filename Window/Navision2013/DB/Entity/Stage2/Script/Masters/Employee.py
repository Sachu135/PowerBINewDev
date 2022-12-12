from pyspark.sql import SparkSession,SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import lit, concat
from pyspark.sql.types import *
import pyspark.sql.functions as F
import os,sys
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
STAGE1_PATH=Kockpit_Path+"/" +DBName+"/" +EntityName+"/" +"Stage1/ParquetData"
STAGE2_PATH=Kockpit_Path+"/" +DBName+"/" +EntityName+"/" +"Stage2/ParquetData"
sqlCtx, spark = getSparkConfig("local[*]", "Stage2:Employee")
def masters_Employee():
    
    for dbe in config["DbEntities"]:
        if dbe['ActiveInactive']=='true' and  dbe['Location']==DBEntity:
            CompanyName=dbe['Name']
            CompanyName=CompanyName.replace(" ","")
            try:
                logger = Logger()   
                finalDF=spark.read.format("parquet").load(STAGE1_PATH+"/Employee")
                finalDF=finalDF.withColumn("DBName",concat(lit(DBName))).withColumn("EntityName",concat(lit(EntityName)))
                result_df = finalDF.select([F.col(col).alias(col.replace(" ","")) for col in finalDF.columns])
                result_df = result_df.select([F.col(col).alias(col.replace("(","")) for col in result_df.columns])
                result_df = result_df.select([F.col(col).alias(col.replace(")","")) for col in result_df.columns])
                result_df.write.option("maxRecordsPerFile", 10000).format("parquet").mode("overwrite").option("overwriteSchema", "true").save(STAGE2_PATH+"/"+"Masters/Employee")
                logger.endExecution()
                try:
                    IDEorBatch = sys.argv[1]
                except Exception as e :
                    IDEorBatch = "IDLE"
            
                log_dict = logger.getSuccessLoggedRecord("Employee", DBName, EntityName, result_df.count(), len(result_df.columns), IDEorBatch)
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
                os.system("spark-submit "+Kockpit_Path+"/Email.py 1 Employee '"+CompanyName+"' "+DBEntity+" "+str(exc_traceback.tb_lineno)+"")   
                log_dict = logger.getErrorLoggedRecord('Employee', '', '', str(ex), exc_traceback.tb_lineno, IDEorBatch)
                log_df = spark.createDataFrame(log_dict, logger.getSchema())
                log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)        
    print('masters_Employee completed: ' + str((dt.datetime.now()-st).total_seconds()))
    
if __name__ == '__main__':
    
    masters_Employee()