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
FilePathSplit = Filepath.split('\\')
DBName = FilePathSplit[-5]
EntityName = FilePathSplit[-4]
DBEntity = DBName+EntityName
STAGE1_Configurator_Path=Kockpit_Path+"/" +DBName+"/" +EntityName+"/" +"Stage1/ConfiguratorData/"
STAGE1_PATH=Kockpit_Path+"/" +DBName+"/" +EntityName+"/" +"Stage1/ParquetData"
STAGE2_PATH=Kockpit_Path+"/" +DBName+"/" +EntityName+"/" +"Stage2/ParquetData"
conf = SparkConf().setMaster("local[16]").setAppName("DSE").\
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
entityLocation =DBName+EntityName 
for dbe in config["DbEntities"]:
    if dbe['ActiveInactive']=='true' and  dbe['Location']==DBEntity:
        CompanyName=dbe['Name']
        CompanyName=CompanyName.replace(" ","")
        try:  
            logger = Logger()
            DSE=spark.read.format("parquet").load(STAGE1_PATH+"/Dimension Set Entry")
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
            finalDF.coalesce(1).write.format("parquet").mode("overwrite").option("overwriteSchema", "true").save(STAGE2_PATH+"/"+"Masters/DSE")
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
