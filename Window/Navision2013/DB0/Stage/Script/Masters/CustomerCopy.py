
from pyspark.sql import SparkSession,SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import *
import pyspark.sql.functions as F
import os,sys
from os.path import dirname, join, abspath
import datetime as dt
Connection =abspath(join(join(dirname(__file__), '..'),'..','..','..','DB1'))
sys.path.insert(0, Connection)
from Configuration.AppConfig import * 
from Configuration.Constant import *
from Configuration.udf import *
# Connection =abspath(join(join(dirname(__file__), '..'),'..','..','..','DB1','E1'))
Abs_Path =abspath(join(join(dirname(__file__), '..'),'..','..','..')) 
Kockpit_Path =abspath(join(join(dirname(__file__), '..'),'..','..'))
DB0 =os.path.split(Kockpit_Path)
DB0 = DB0[1]

owmode = 'overwrite'
apmode = 'append'                           
st = dt.datetime.now()
conf = SparkConf().setMaster("local[*]").setAppName("Customer")
sc = SparkContext(conf = conf)
sqlCtx = SQLContext(sc)
spark = sqlCtx.sparkSession
ConfTab='tblCompanyName'
Query="(SELECT *\
                FROM "+ConfiguratorDbInfo.Schema+"."+chr(34)+ConfTab+chr(34)+") AS df"
Configurator_Data = spark.read.format("jdbc").options(url=ConfiguratorDbInfo.PostgresUrl, dbtable=Query,user=ConfiguratorDbInfo.props["user"],password=ConfiguratorDbInfo.props["password"],driver= ConfiguratorDbInfo.props["driver"]).load()
Configurator_Data.show() 
# for dbe in Configurator_Data:
#     print(Configurator_Data['ActiveCompany'])
#     if Configurator_Data['ActiveCompany']!='FAL':
#         DBName=Configurator_Data['DBName']
#         
#         EntityName =Configurator_Data['DBName']
#         print(DBName)
#         print(EntityName)
# exit()
for dbe in config["DbEntities"]:
    if dbe['ActiveInactive']=='true':
        CompanyName=dbe['Name']
        CompanyName=CompanyName.replace(" ","")   
#         try:
#             cur = conn.cursor()
#             cur.execute('DROP TABLE IF EXISTS Masters.Customer;')  
#             conn.commit()
#             conn.close()
#         except Exception as ex:
#             exc_type,exc_value,exc_traceback=sys.exc_info()
#             print("Error:",ex)
#             print("type - "+str(exc_type))
#             print("File - "+exc_traceback.tb_frame.f_code.co_filename)
#             print("Error Line No. - "+str(exc_traceback.tb_lineno))
        try:
            logger =Logger()
            ConfTab='tblCompanyName'
            Query="(SELECT *\
                            FROM "+ConfiguratorDbInfo.Schema+"."+chr(34)+ConfTab+chr(34)+") AS df"
            CompanyDetail = spark.read.format("jdbc").options(url=ConfiguratorDbInfo.PostgresUrl, dbtable=Query,user=ConfiguratorDbInfo.props["user"],password=ConfiguratorDbInfo.props["password"],driver= ConfiguratorDbInfo.props["driver"]).load()

#             CompanyDetail =spark.read.parquet(Path)
            CompanyDetail.show()
            CompanyDetail=CompanyDetail.filter((CompanyDetail['ActiveCompany']=='true') | (CompanyDetail['ActiveCompany']=='false'))
            CompanyDetail.show()
            NoofRows = CompanyDetail.count()  
            print(NoofRows) 
            for i in range(NoofRows): 
#                 CompanyDetail.collect()[i]['DBName'])  
                DBName=(CompanyDetail.collect()[i]['DBName'])
                EntityName =(CompanyDetail.collect()[i]['NewCompanyName'])
                Path = Abs_Path+"/"+DBName+"/"+EntityName+"\\Stage2\\ParquetData\\Masters\Customer"
                print("74")
                print(Path)
                
                finalDF=spark.read.parquet(Path)
                finalDF.show()
                print(i)
                print(finalDF.count())
                if os.path.exists(Path):
                    
                    finalDF=spark.read.parquet(Path)
                    finalDF.show()
                    if i==0:
                        finalDF.write.jdbc(url=PostgresDbInfo.PostgresUrl , table="Masters.Customer", mode=owmode, properties=PostgresDbInfo.props)
                    else:
                        finalDF.write.jdbc(url=PostgresDbInfo.PostgresUrl , table="Masters.Customer", mode=apmode, properties=PostgresDbInfo.props)
                
                logger.endExecution()
                try:
                    IDEorBatch = sys.argv[1]
                except Exception as e :
                    IDEorBatch = "IDLE"
            
                log_dict = logger.getSuccessLoggedRecord("Masters.Customer", DB0," ", finalDF.count(), len(finalDF.columns), IDEorBatch)
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
#             os.system("spark-submit D:\AMIT_KUMAR\Kockpit\Email.py 1 Customer '"+CompanyName+"' "+DB0+" "+str(exc_traceback.tb_lineno)+"")   
            log_dict = logger.getErrorLoggedRecord('Masters.Customer', DB0," ", str(ex), exc_traceback.tb_lineno, IDEorBatch)
            log_df = spark.createDataFrame(log_dict, logger.getSchema())
            log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)        
print('masters_CUSTOMER completed: ' + str((dt.datetime.now()-st).total_seconds()))     
      
      