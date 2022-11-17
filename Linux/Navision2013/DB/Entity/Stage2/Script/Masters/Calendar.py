
from pyspark.sql import SparkSession,SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import lit,concat,month,year,substring,when,ceil,udf
from pyspark.sql.types import *
import pyspark.sql.functions as F
import os,sys
from os.path import dirname, join, abspath
import datetime as dt 
from builtins import str
import pandas
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
STAGE1_Configurator_Path=HDFS_PATH+DIR_PATH+"/" +DBName+"/" +EntityName+"/" +"Stage1/ConfiguratorData/"
STAGE1_PATH=HDFS_PATH+DIR_PATH+"/" +DBName+"/" +EntityName+"/" +"Stage1/ParquetData"
STAGE2_PATH=HDFS_PATH+DIR_PATH+"/" +DBName+"/" +EntityName+"/" +"Stage2/ParquetData"

conf = SparkConf().setMaster(SPARK_MASTER).setAppName("Calendar")\
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


for dbe in config["DbEntities"]:
    if dbe['ActiveInactive']=='true' and  dbe['Location']==DBEntity:
        CompanyName=dbe['Name']
        CompanyName=CompanyName.replace(" ","")
        
        try:
            logger = Logger()
            Company =spark.read.format("delta").load(STAGE1_Configurator_Path+"/tblCompanyName")
            Company = Company.filter(Company['ActiveCompany']==True)\
                                    .select('DBName','NewCompanyName','CompanyName','StartDate','EndDate')\
                                    .withColumnRenamed('NewCompanyName','EntityName')  
            df = Company.select("StartDate","EndDate")
            Calendar_StartDate = df.select(df.StartDate).collect()[0]["StartDate"]
            Calendar_StartDate = datetime.datetime.strptime(Calendar_StartDate,"%Y-%m-%d").date()
            Calendar_EndDate = datetime.datetime.today().date() 
            
            data =[]
            for single_date in DRANGE(Calendar_StartDate, Calendar_EndDate):
                data.append({'Link_date':single_date})
            schema = StructType([
            StructField("Link_date", DateType(),True)
            ])
            records=spark.createDataFrame(data,schema)
            records=records.select(records.Link_date.alias('LinkDate'),month(records.Link_date).alias('Month'),year(records.Link_date).alias('Year')).distinct().sort('LinkDate')
            records = records.withColumn("Fiscal_Monthno",when(records.Month>(int(MnSt)-1),records.Month-int(MnSt)+1).otherwise(records.Month+(13-int(MnSt))))\
                        .withColumn("Fiscal_Year",when(records.Month>(int(MnSt)-1),concat(records.Year,lit('-'),substring(records.Year+1,3,2))).otherwise(concat(records.Year-1,lit('-'),substring(records.Year,3,2))))\
                        .withColumn("FY_Year",when(records.Month>(int(MnSt)-1),records.Year).otherwise(records.Year-1))
            
            records = records.na.fill({'LinkDate':'NA'})
            records = records.withColumn("Fiscal_Quarter",when(records.Month>(int(MnSt)-1),concat(lit("Q"),ceil((records.Month-int(MnSt)+1)/3))).otherwise(concat(lit("Q"),ceil((records.Month+(13-int(MnSt)))/3))))

            records = records.withColumn("LinkDateKey",concat(lit(DBName),lit("|"),lit(EntityName),lit("|"),records.LinkDate))\
                .withColumn("DBName",lit(DBName))\
                .withColumn("EntityName",lit(EntityName))

            records = records.withColumn("Fiscal_Month",when(records.Month == 1,"Jan").when(records.Month == 2,"Feb").when(records.Month == 3,"Mar").when(records.Month == 4,"Apr").when(records.Month == 5,"May").when(records.Month == 6,"Jun").when(records.Month == 7,"Jul").when(records.Month == 8,"Aug").when(records.Month == 9,"Sep").when(records.Month == 10,"Oct").when(records.Month == 11,"Nov").when(records.Month == 12,"Dec").otherwise('null'))
            records = records.withColumn("Reload_Time",lit(datetime.datetime.now()))

            records.coalesce(1).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(STAGE2_PATH+"/"+"Masters/Calendar")
            logger.endExecution()
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"
            log_dict = logger.getSuccessLoggedRecord("Calendar", DBName, EntityName, records.count(), len(records.columns), IDEorBatch)
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
                os.system("spark-submit "+Kockpit_Path+"/Email.py 1 Calendar '"+CompanyName+"' "+DBEntity+" "+str(exc_traceback.tb_lineno)+"")
            
                log_dict = logger.getErrorLoggedRecord('Calendar', '', '', str(ex), exc_traceback.tb_lineno, IDEorBatch)
                log_df = spark.createDataFrame(log_dict, logger.getSchema())
                log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)
print('masters_calendar completed: ' + str((dt.datetime.now()-st).total_seconds()))