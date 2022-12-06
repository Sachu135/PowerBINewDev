from pyspark.sql import SparkSession,SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import *
import pyspark.sql.functions as F
import os,sys
from os.path import dirname, join, abspath
import datetime as dt
root_directory =abspath(join(join(dirname(__file__), '..'),'..','..','..',))
root_directory=root_directory+"/"
DBList=[]
for folders in os.listdir(root_directory):
    if os.path.isdir(os.path.join(root_directory,folders)):
        if 'DB' in folders:
            if 'DB0' in folders:
                pass
            else:
                DBList.insert(0,folders )
Connection =abspath(join(join(dirname(__file__), '..'),'..','..','..',DBList[0]))
sys.path.insert(0, Connection)
from Configuration.Constant import *
from Configuration.udf import *
Abs_Path =abspath(join(join(dirname(__file__), '..'),'..','..','..')) 
Kockpit_Path =abspath(join(join(dirname(__file__), '..'),'..','..','..'))
DBO_Path=abspath(join(join(dirname(__file__), '..'),'..','..'))
DB0 =os.path.split(DBO_Path)
DB0 = DB0[1]
owmode = 'overwrite'
apmode = 'append'                           
st = dt.datetime.now()
sqlCtx,spark=getSparkConfig(SPARK_MASTER, "StageDB0:Finance-MISPNL")
def finance_MISPNL():
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    ConfTab='tblCompanyName'
    try:
        finalDF = spark.createDataFrame([], StructType([]))
        Query="(SELECT *\
                        FROM "+ConfiguratorDbInfo.Schema+"."+chr(34)+ConfTab+chr(34)+") AS df"
        CompanyDetail = spark.read.format("jdbc").options(url=ConfiguratorDbInfo.PostgresUrl, dbtable=Query,user=ConfiguratorDbInfo.props["user"],password=ConfiguratorDbInfo.props["password"],driver= ConfiguratorDbInfo.props["driver"]).load()
        CompanyDetail=CompanyDetail.filter((CompanyDetail['ActiveCompany']=='true'))
        for d in range(len(DBList)):  
            DB=DBList[d]
            logger =Logger()
            Query="(SELECT *\
                        FROM "+ConfiguratorDbInfo.Schema+"."+chr(34)+ConfTab+chr(34)+") AS df"
            CompanyDetail = spark.read.format("jdbc").options(url=ConfiguratorDbInfo.PostgresUrl, dbtable=Query,user=ConfiguratorDbInfo.props["user"],password=ConfiguratorDbInfo.props["password"],driver= ConfiguratorDbInfo.props["driver"]).load()
            CompanyDetail=CompanyDetail.filter((CompanyDetail['ActiveCompany']=='true'))
            CompanyDetail=CompanyDetail.filter((CompanyDetail['DBName']==DB))
            NoofRows = CompanyDetail.count()  
            
            for i in range(NoofRows): 
                
                    DBName=(CompanyDetail.collect()[i]['DBName'])
                    EntityName =(CompanyDetail.collect()[i]['NewCompanyName'])
                    CompanyName=(CompanyDetail.collect()[i]['CompanyName'])
                    DBE=DBName+EntityName
                    CompanyName=CompanyName.replace(" ","")
                    Path = HDFS_PATH+DIR_PATH+"/"+DBName+"/"+EntityName+"/Stage2/ParquetData/Finance/MISPNL"
                    fe = fs.exists(spark._jvm.org.apache.hadoop.fs.Path(Path))
                    if(fe):
                        finalDF1=spark.read.format("delta").load(Path)
                        if (d==0) & (i==0):
                            finalDF=finalDF1
                            
                        else:
                            finalDF=finalDF.unionByName(finalDF1,allowMissingColumns=True)
                            
                    else:
                        print("MISPNL "+DBName+EntityName+" Does not exist")
        finalDF.write.jdbc(url=PostgresDbInfo.PostgresUrl , table="Finance.MISPNL", mode=owmode, properties=PostgresDbInfo.props)
        logger.endExecution()
        try:
            IDEorBatch = sys.argv[1]
        except Exception as e :
            IDEorBatch = "IDLE"
    
        log_dict = logger.getSuccessLoggedRecord("Finance.MISPNL", DB0, "", finalDF.count(), len(finalDF.columns), IDEorBatch)
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
        DBE=DBName+EntityName
        os.system("spark-submit "+Kockpit_Path+"\Email.py 1 MISPNL "+CompanyName+" "" "+str(exc_traceback.tb_lineno)+"")   
        log_dict = logger.getErrorLoggedRecord('Finance.MISPNL', DB0, "" , str(ex), exc_traceback.tb_lineno, IDEorBatch)
        log_df = spark.createDataFrame(log_dict, logger.getSchema())
        log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)        
    print('Finance MISPNL completed: ' + str((dt.datetime.now()-st).total_seconds()))  
if __name__ == "__main__":
    finance_MISPNL()      