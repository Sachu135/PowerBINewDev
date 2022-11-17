from pyspark.sql import SparkSession,SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import lit
import time,re,os,sys,traceback
from os.path import dirname, join, abspath
import datetime as dt 
from builtins import str
import pandas
from pyspark.sql.functions import  when,concat_ws,col
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
conf = SparkConf().setMaster(SPARK_MASTER).setAppName("DSE")\
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
entityLocation =DBName+EntityName 
for dbe in config["DbEntities"]:
    if dbe['ActiveInactive']=='true' and  dbe['Location']==DBEntity:
        CompanyName=dbe['Name']
        CompanyName=CompanyName.replace(" ","")
        try:  
            logger = Logger()
            DSE=spark.read.format("delta").load(STAGE1_PATH+"/Dimension Set Entry")
            DSE = DSE.select('DimensionSetID','DimensionCode','DimensionValueCode').distinct()
            query_DSE_ID = DSE.select('DimensionSetID').distinct()
            for i in config["Dimensions"]:
                if i['DBEntity']==entityLocation:
                    ActiveDimension = i['Active Dimension']
                    
            NoOfRows = len(ActiveDimension)
            for i in range(0,NoOfRows):
                var = ActiveDimension[i]
                temp_dim = re.sub('[\s+]', '', var)
                vdim = 'Link_'+temp_dim
                temp_table = DSE.filter(DSE['DimensionCode'] == var)
                query_DSE_ID = FULL(query_DSE_ID,temp_table,'DimensionSetID')
                
                query_DSE_ID = query_DSE_ID.withColumnRenamed('DimensionValueCode',vdim)
                query_DSE_ID = query_DSE_ID.withColumn(vdim+'Key',when(query_DSE_ID[vdim].isNull(),query_DSE_ID[vdim])\
                                    .otherwise(concat_ws('|',lit(DBName),lit(EntityName),col(vdim))))\
                                    .drop('DimensionCode')    
            finalDF = query_DSE_ID.withColumn("EntityName",lit(EntityName)).withColumn("DBName",lit(DBName))
            finalDF.coalesce(1).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(STAGE2_PATH+"/"+"Masters/DSE")
            logger.endExecution()
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"
        
            log_dict = logger.getSuccessLoggedRecord("DSE", DBName, EntityName, finalDF.count(), len(finalDF.columns), IDEorBatch)
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
            os.system("spark-submit "+Kockpit_Path+"/Email.py 1 DSE '"+CompanyName+"' "+DBEntity+" "+str(exc_traceback.tb_lineno)+"")    
            log_dict = logger.getErrorLoggedRecord('DSE', '', '', str(ex), exc_traceback.tb_lineno, IDEorBatch)
            log_df = spark.createDataFrame(log_dict, logger.getSchema())
            log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)        
print('masters_DSE completed: ' + str((dt.datetime.now()-st).total_seconds()))
