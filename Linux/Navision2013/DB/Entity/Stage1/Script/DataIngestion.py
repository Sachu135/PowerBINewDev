from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession,Row
from pyspark.sql import functions as f
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import regexp_replace, col, udf, broadcast,when,lit
import datetime, random
import datetime as dt
import multiprocessing
import threading, queue
from threading import Lock
from multiprocessing.pool import ThreadPool
import psycopg2
from pyspark.sql import functions as F
import pandas as pd
import os,sys,subprocess
from os.path import dirname, join, abspath
helpersDir = abspath(join(join(dirname(__file__), '..'),'..','..'))
sys.path.insert(0, helpersDir)
Abs_Path =abspath(join(join(dirname(__file__),'..'),'..'))
Kockpit_Path =abspath(join(join(dirname(__file__),'..','..','..','..')))
st = dt.datetime.now()
from Configuration import  AppConfig as ac
DBNamepath= abspath(join(join(dirname(__file__), '..'),'..','..'))
EntityNamepath=abspath(join(join(dirname(__file__), '..'),'..'))
DBName =os.path.split(DBNamepath)
EntityName =os.path.split(EntityNamepath)
DBName = DBName[1]
EntityName=EntityName[1]
ENTITY_LOCATION_LOCAL=DBName+EntityName
entityLocation = DBName+EntityName
from Configuration.Constant import *
conf =  SparkConf().setMaster(SPARK_MASTER).setAppName("DataIngestion")\
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
                .set("spark.kryoserializer.buffer.max","512m")\
                .set("spark.cores.max","24")\
                .set("spark.executor.memory","8g")\
                .set("spark.sql.debug.maxToStringFields","500")\
                .set("spark.driver.memory","24g")\
                .set("spark.driver.maxResultSize","20g")\
                .set("spark.memory.offHeap.enabled",'true')\
                .set("spark.memory.offHeap.size","100g")\
                .set('spark.scheduler.mode', 'FAIR')\
                .set("spark.sql.broadcastTimeout", "36000")\
                .set("spark.network.timeout", 10000000)\
                .set("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0")\
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
for dbe in ac.config["DbEntities"]:
    select_list = ['Name','Year', 'Month']
    res = [dbe[i] for i in select_list if i in dbe]
    CompanyName = res[0]
    CompanyNamewos=CompanyName.replace(" ","")
successViewCounter = 0
totalViews = 0
lock = Lock()
def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]

def udfBinaryToReadableFunc(byteArray):
    st = ''
    for itm in byteArray:
        st += str(itm)
    return st

udfBinaryToReadable = f.udf(lambda a: udfBinaryToReadableFunc(a))
schemaForMetadata = StructType([
        StructField("dbname", StringType(),False),
        StructField("tblname", StringType(),False),
        StructField("colname", StringType(),False),
        StructField("fullname", StringType(),False)
    ])
def updateSchemaMetadata(dirnameInHDFS, tblNameInHDFS, tableColumns):
    try:
        delQuery = 'DELETE FROM stage1schema where "dbname" = \'' + dirnameInHDFS + '\' and "tblname" = \'' + tblNameInHDFS + '\''
        con = psycopg2.connect(database=PostgresDbInfo.MetadataDB, user=PostgresDbInfo.props["user"], password=PostgresDbInfo.props["password"], host=PostgresDbInfo.Host, port=PostgresDbInfo.Port)
        cur = con.cursor()
        cur.execute(delQuery)
        con.commit()
        con.close()
        data =[]
        for col in tableColumns:
            fullName = "delta.`" + STAGE1_PATH + "/ParquetData/" + tblNameInHDFS + "`"
            data.append({'dbname':dirnameInHDFS, 'tblname':tblNameInHDFS, 'colname':col, 'fullname':fullName})
        SchemaDataDF=spark.createDataFrame(data, schemaForMetadata)
        SchemaDataDF.write.jdbc(url=PostgresDbInfo.MetadataDBUrl, table="stage1schema", mode='append', properties=PostgresDbInfo.props)
    except Exception as ex:
        print("Error in updating Schema",str(ex))
        data =[]
        for col in tableColumns:
            fullName = "delta.`" + STAGE1_PATH + "/ParquetData/" + tblNameInHDFS + "`"
            data.append({'dbname':dirnameInHDFS, 'tblname':tblNameInHDFS, 'colname':col, 'fullname':fullName})
        SchemaDataDF=spark.createDataFrame(data, schemaForMetadata)
        SchemaDataDF.write.jdbc(url=PostgresDbInfo.MetadataDBUrl, table="stage1schema", mode='overwrite', properties=PostgresDbInfo.props)
def dataFrameWriterIntoHDFS(colSelector, tblFullName, sqlurl, hdfsLoc, dirnameInHDFS, tblNameInHDFS):
    tblQuery = "(SELECT "+ colSelector +" FROM ["+tblFullName+"]) AS FullWrite"
    tableDataDF = spark.read.format(ConnectionInfo.JDBC_PARAM).options(url=sqlurl,dbtable=tblQuery,driver=ConnectionInfo.SQL_SERVER_DRIVER,fetchsize=100000).load() 
    tableDataDF = tableDataDF.select([F.col(col).alias(col.replace(' ', '').replace('(', '').replace(')', '')) for col in tableDataDF.columns])
    tableDataDF.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(hdfsLoc)
    updateSchemaMetadata(dirnameInHDFS, tblNameInHDFS, tableDataDF.columns)
def schemaWriterIntoHDFS(sqlurl, HDFSDatabase, SQLNAVSTRING):
    HDFS_Schema_Path = STAGE1_PATH +"/Schema"
    fullschemaQuery = "(SELECT COLUMN_NAME,DATA_TYPE,TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WITH (NOLOCK) where TABLE_NAME like '" + SQLNAVSTRING + "%') AS FullSchema"
    fullschema = spark.read.format(ConnectionInfo.JDBC_PARAM).options(url=sqlurl,dbtable=fullschemaQuery,driver=ConnectionInfo.SQL_SERVER_DRIVER,fetchsize=10000).load() #numPartitions="200"
    fullschema.cache()
    fullschema.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(HDFS_Schema_Path)
def task(entity, Table, sqlurl, hdfsLocation,Schema_DF):
    table_name = entity + Table['Table']
    HDFS_Path =  HDFS_PATH+DIR_PATH + hdfsLocation + "/Stage1/ParquetData/" + Table['Table']
    try:
        logger=Logger()
        st = dt.datetime.now()
        schema = Schema_DF.filter(Schema_DF['TABLE_NAME']==table_name).select('COLUMN_NAME','DATA_TYPE')
        schema=schema.filter(schema['COLUMN_NAME'].isin(Table['Columns']))
        schema_Columns = schema.withColumn('COLUMN_NAME',regexp_replace(schema['COLUMN_NAME'],"[( )]",""))
        schema_Columns = schema_Columns.withColumn('DATA_TYPE',when(schema_Columns['DATA_TYPE'].isin(['nvarchar','varchar','sql_variant']),lit('string'))\
                                                   .when(schema_Columns['DATA_TYPE']=='decimal',lit('decimal(38,20)'))\
                                                   .when(schema_Columns['DATA_TYPE']=='tinyint',lit('int'))\
                                                   .when(schema_Columns['DATA_TYPE']=='datetime',lit('timestamp'))\
                                                   .when(schema_Columns['DATA_TYPE'].isin(['image','varbinary']),lit('binary'))\
                                                   .when(schema_Columns['DATA_TYPE']=='uniqueidentifier',lit('string'))\
                                                   .otherwise(schema_Columns['DATA_TYPE']))
        schema_Columns = schema_Columns.withColumn('DATA_TYPE',when(schema_Columns['COLUMN_NAME']=='timestamp',lit('bigint'))\
                                                            .otherwise(schema_Columns['DATA_TYPE']))
        schema = schema.collect()
        schemaJoiner = [row.COLUMN_NAME+"#"+row.DATA_TYPE for row in schema_Columns.collect()]
        
        incrementalLoadColumn = ''
        if 'CheckOn' in Table:
            incrementalLoadColumn = Table['CheckOn']  
        temp = ''
        for i in range(0,len(schema)):
            col_name_temp = "["+schema[i]['COLUMN_NAME']+"]"
            if(i == 0 or schema[i]['DATA_TYPE'] == 'sql_variant'):
                col_name_temp = col_name_temp
            elif(schema[i]['DATA_TYPE'] != 'sql_variant'):
                col_name_temp = "," + col_name_temp
  
            if(i == 0 and schema[i]['DATA_TYPE'] == 'sql_variant'):
                            
                temp =temp +"CONVERT(varchar,"+col_name_temp+",20) as "+col_name_temp
            elif schema[i]['DATA_TYPE']=='sql_variant':
                            
                temp =temp +",CONVERT(varchar,"+col_name_temp+",20) as "+col_name_temp
            elif incrementalLoadColumn == schema[i]['COLUMN_NAME']:
                temp = temp + (',' if i > 0 else '') + 'CAST([' + schema[i]['COLUMN_NAME'] + '] AS BIGINT) AS [' + schema[i]['COLUMN_NAME'] + ']'
            else:
                temp = temp + col_name_temp

        tableQuery = "(SELECT top 1 "+temp+" FROM ["+table_name+"] WITH (NOLOCK)) AS GetColumns" 
        schemaDF = spark.read.format(ConnectionInfo.JDBC_PARAM).options(url=sqlurl,dbtable=tableQuery,driver=ConnectionInfo.SQL_SERVER_DRIVER,fetchsize=10000).load()
        schemaDF = schemaDF.select([F.col(col).alias(col.replace(' ', '').replace('(', '').replace(')', '')) for col in schemaDF.columns])
        schemaJoiner = [x+'#'+y for x,y in schemaDF.dtypes]
        schemaStr = "|".join(schemaJoiner)
        dataFrameWriterIntoHDFS(temp, table_name, sqlurl, HDFS_Path, hdfsLocation, Table['Table'])
        try:
            if 'Key' in Table and 'CheckOn' in Table:
                try:
                    hdfsDF = spark.read.format("delta").load(HDFS_Path)
                    hdfsDF.cache()   
                except Exception as e:
                    print(table_name + ": Table is not present in HDFS, So creating in HDFS")
                    print("Need a full write")
                    dataFrameWriterIntoHDFS(temp, table_name, sqlurl, HDFS_Path, hdfsLocation, Table['Table'])
                else:
                    hdfsSchemaJoiner = [x+'#'+y for x,y in hdfsDF.dtypes]
                    hdfsSchemaStr = "|".join(hdfsSchemaJoiner)
                    if(schemaStr == hdfsSchemaStr):
                        maxVal = hdfsDF.agg({Table['CheckOn']: "max"}).collect()[0][0]                            
                        if maxVal is None:
                            dataFrameWriterIntoHDFS(temp, table_name, sqlurl, HDFS_Path, hdfsLocation, Table['Table'])
                            return
                        tableQuery = "(SELECT "+temp+" FROM ["+table_name+"] WHERE CAST([" + Table['CheckOn'] + "] AS BIGINT) > " + str(maxVal) + ") AS data"
                        tableDataDF = spark.read.format(ConnectionInfo.JDBC_PARAM).options(url=sqlurl,dbtable=tableQuery,driver=ConnectionInfo.SQL_SERVER_DRIVER,fetchsize=10000).load()
                        tableDataDF = tableDataDF.select([F.col(col).alias(col.replace(' ', '').replace('(', '').replace(')', '')) for col in tableDataDF.columns])
                        tableDataDF.cache()
                        dtTable=DeltaTable.forPath(spark, HDFS_Path)
                        dtTable.vacuum(0.1)
                        mergeConditionsList = []
                        keyNames = []
                        modifiedKeyNames = []
                        for key in Table['Key']:
                            mergeConditionsList.append("(source." + key.replace(' ', '').replace('(', '').replace(')', '') + " = dest." + key.replace(' ', '').replace('(', '').replace(')', '') + ")")
                            keyNames.append(("[" + key + "]"))
                            modifiedKeyNames.append(key.replace(' ', '').replace('(', '').replace(')', ''))
                            
                        mergeCondition = ("" + " and ".join(mergeConditionsList) + "") if len(mergeConditionsList) > 1 else "".join(mergeConditionsList) 
                        if tableDataDF.count() > 0:
                            dtTable.alias("source").merge(tableDataDF.alias("dest"), mergeCondition).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()                            
                            updateSchemaMetadata(hdfsLocation, Table['Table'], tableDataDF.columns)
                           
                        else:
                            print(table_name+ ': No UpSert required')   
                        hdfsDF = spark.read.format("delta").load(HDFS_Path)                        
                        tempForKeys = ''                     
                        for i in range(0,len(schema)):
                            col_name_temp = "["+schema[i]['COLUMN_NAME']+"]"
                           
                            if col_name_temp in keyNames:
                                if schema[i]['DATA_TYPE'] =='sql_variant':
                                    tempForKeys = tempForKeys + (' , ' if len(tempForKeys) > 0 else '') + "CONVERT(varchar,"+col_name_temp+",20) as "+col_name_temp
                                else:
                                    tempForKeys = tempForKeys + (' , ' if len(tempForKeys) > 0 else '') + col_name_temp

                        tableQuery = "(SELECT " + tempForKeys + " FROM ["+table_name+"]) AS data1"
                        onlyIDsDF = spark.read.format(ConnectionInfo.JDBC_PARAM).options(url=sqlurl,dbtable=tableQuery,driver=ConnectionInfo.SQL_SERVER_DRIVER,fetchsize=100000).load()
                        onlyIDsDF = onlyIDsDF.select([F.col(col).alias(col.replace(' ', '').replace('(', '').replace(')', '')) for col in onlyIDsDF.columns])                         
                        recordsToBeDeleteDF = hdfsDF.alias("a").join(onlyIDsDF.alias("b"), modifiedKeyNames, 'left').where(onlyIDsDF[onlyIDsDF.columns[0]].isNull()).selectExpr("a.*")
                        recordsToBeDeleteDFCount = recordsToBeDeleteDF.count()
                        mergeConditionsList.append("(source." + incrementalLoadColumn.replace(' ', '').replace('(', '').replace(')', '') + " = dest." + incrementalLoadColumn.replace(' ', '').replace('(', '').replace(')', '') + ")")
                        mergeConditionForDeletion = ("" + " and ".join(mergeConditionsList) + "") if len(mergeConditionsList) > 1 else "".join(mergeConditionsList) 
                        if recordsToBeDeleteDFCount > 0:
                            hdfsDF = DeltaTable.forPath(spark, HDFS_Path);
                            hdfsDF.alias("source").merge(recordsToBeDeleteDF.alias("dest"), mergeConditionForDeletion).whenMatchedDelete().execute();
                        hdfsDF = spark.read.format("delta").load(HDFS_Path) 
                        
                    else:
                        dataFrameWriterIntoHDFS(temp, table_name, sqlurl, HDFS_Path, hdfsLocation, Table['Table'])
                        
                    hdfsDF.unpersist()
            else:
                dataFrameWriterIntoHDFS(temp, table_name, sqlurl, HDFS_Path, hdfsLocation, Table['Table'])### writing the tables in the hdfslocatiom
        except Exception as ex:
            print("For Table: " + str(Table))
            exc_type,exc_value,exc_traceback=sys.exc_info()
            print("Error: " + table_name, ex)
            print("type - " + str(exc_type))
            print("File - " + exc_traceback.tb_frame.f_code.co_filename)
            print("Error Line No. - " + table_name + " " + str(exc_traceback.tb_lineno))
                 
        ts = (dt.datetime.now()-st).total_seconds()
    except Exception as ex:
        print('Error in Task: ', Table, ' -> ', str(ex))
    
jobs = []
moduleName = sys.argv[1] if len(sys.argv) > 1 else ''

for entityObj in ac.config["DbEntities"]:
    entity = entityObj["Name"]
    entityLoc = entityObj["Location"]
    DB=entityLoc[0:3]
    EntityName=entityLoc[3:6]
    entityLocation=DB+"/"+EntityName
    dbName = entityObj["DatabaseName"]
    if entityLoc==ENTITY_LOCATION_LOCAL:
        STAGE1_PATH=HDFS_PATH+DIR_PATH+DB+"/"+EntityName+"/Stage1"
        STAGE1_Configurator_Path=STAGE1_PATH+"/ConfiguratorData"
        hdfspath = STAGE1_PATH 
        sqlurl=ConnectionInfo.SQL_URL.format(ac.config["SourceDBConnection"]['url'], str(ac.config["SourceDBConnection"]["port"]), dbName, ac.config["SourceDBConnection"]['userName'], ac.config["SourceDBConnection"]['password'],ac.config["SourceDBConnection"]['dbtable'])
        schemaWriterIntoHDFS(sqlurl, entityLocation, entity)
        HDFS_Schema_DF = sqlCtx.sparkSession.read.format("delta").load(hdfspath + "/Schema")
        query = "(SELECT name from sys.tables where name like '" + entity + "%') as data"
        viewDF = spark.read.format(ConnectionInfo.JDBC_PARAM).options(url=sqlurl,dbtable=query,driver=ConnectionInfo.SQL_SERVER_DRIVER,fetchsize=10000).load()
        list_of_tables = viewDF.select(viewDF.name).rdd.flatMap(lambda x: x).collect()
        for Table in ac.config["TablesToIngest"]:
            try:
                logger=Logger()
                table_name = entity + Table['Table']
                if (len(moduleName) == 0 and table_name in list_of_tables) \
                    or (len(moduleName) > 0 and 'Modules' in Table and moduleName in Table['Modules'] and table_name in list_of_tables) \
                    or (len(moduleName) > 0 and 'Modules' not in Table and table_name in list_of_tables):

                    try:             
                        t = threading.Thread(target=task, args=(entity, Table, sqlurl, entityLocation,HDFS_Schema_DF))
                        jobs.append(t)
                        logger.endExecution()
                        try:
                                IDEorBatch = sys.argv[1]
                        except Exception as e :
                            IDEorBatch = "IDLE"

                        log_dict = logger.getSuccessLoggedRecord("DataIngestion_"+table_name, DBName, EntityName, viewDF.count(), len(viewDF.columns), IDEorBatch)
                        log_df = spark.createDataFrame(log_dict, logger.getSchema())
                        log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)                        
                    except Exception as ex:
                        exc_type,exc_value,exc_traceback=sys.exc_info()
                        print(str(ex))
                        print("type - "+str(exc_type))
                        print("File - "+exc_traceback.tb_frame.f_code.co_filename)
                        print("Error Line No. - "+str(exc_traceback.tb_lineno))
                        ex = str(ex)
                        logger.endExecution()
                        try:
                            IDEorBatch = sys.argv[1]
                        except Exception as e :
                            IDEorBatch = "IDLE"
                        os.system("spark-submit "+Kockpit_Path+"/Email.py 1  DataIngestion "+CompanyNamewos+" "+entityLocation+" "+str(exc_traceback.tb_lineno)+"") 
                        log_dict = logger.getErrorLoggedRecord('DataIngestion'+table_name, '', '', str(ex), exc_traceback.tb_lineno, IDEorBatch)
                        log_df = spark.createDataFrame(log_dict, logger.getSchema())
                        log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)  
                else:
                    print(table_name+' table name does not exist in SQL Database')

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
                os.system("spark-submit "+Kockpit_Path+"/Email.py 1  DataIngestion "+CompanyNamewos+" "+entityLocation+" "+str(exc_traceback.tb_lineno)+"")        
                log_dict = logger.getErrorLoggedRecord('DataIngestion','', '', str(ex), exc_traceback.tb_lineno, IDEorBatch)
                log_df = spark.createDataFrame(log_dict, logger.getSchema())
                log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)
        
print('jobs count', len(jobs))
def Configurator(ConfTab, STAGE1_Configurator_Path,DB,EntityName):
    Query="(SELECT *\
                FROM "+ConfiguratorDbInfo.Schema+"."+chr(34)+ConfTab+chr(34)+") AS df"
    Configurator_Data = spark.read.format("jdbc").options(url=ConfiguratorDbInfo.PostgresUrl, dbtable=Query,user=ConfiguratorDbInfo.props["user"],password=ConfiguratorDbInfo.props["password"],driver= ConfiguratorDbInfo.props["driver"]).load()
    
    Configurator_Data = Configurator_Data.select([F.col(col).alias(col.replace(" ","")) for col in Configurator_Data.columns])
    Configurator_Data = Configurator_Data.select([F.col(col).alias(col.replace("(","")) for col in Configurator_Data.columns])
    Configurator_Data = Configurator_Data.select([F.col(col).alias(col.replace(")","")) for col in Configurator_Data.columns])
    if all(x in Configurator_Data.columns for x in ['DBName','EntityName']):
        Configurator_Data=Configurator_Data.filter((Configurator_Data['DBName']==DB) & (Configurator_Data['EntityName']==EntityName))
    Configurator_Data.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(STAGE1_Configurator_Path+ConfTab)

table_names = spark.read.format("jdbc").options(url=ConfiguratorDbInfo.PostgresUrl,dbtable='information_schema.tables',user=ConfiguratorDbInfo.props["user"],password=ConfiguratorDbInfo.props["password"],driver=ConfiguratorDbInfo.props["driver"]).load().\
filter("table_schema = '"+ConfiguratorDbInfo.Schema+"'").select("table_name")
table_names_list = [row.table_name for row in table_names.collect()]

for entityObj in ac.config["DbEntities"]:
    entity = entityObj["Name"]
    entityLoc = entityObj["Location"]
    DB=entityLoc[0:3]
    EntityName=entityLoc[3:6]
    entityLocation=DB+"/"+EntityName
    dbName = entityObj["DatabaseName"]
    STAGE1_PATH=HDFS_PATH+DIR_PATH+DB+"/"+EntityName+"/Stage1"
    STAGE1_Configurator_Path=STAGE1_PATH+"/ConfiguratorData/"
    
    if entityLoc==ENTITY_LOCATION_LOCAL:
        for ConfTab in table_names_list:
            try:                         
                t1 = threading.Thread(target=Configurator, args=(ConfTab, STAGE1_Configurator_Path,DB,EntityName))
                jobs.append(t1)
            except Exception as ex:
                exc_type,exc_value,exc_traceback=sys.exc_info()
                print(str(ex))
                print("type - "+str(exc_type))
                print("File - "+exc_traceback.tb_frame.f_code.co_filename)
                print("Error Line No. - "+str(exc_traceback.tb_lineno))
                os.system("spark-submit "+Kockpit_Path+"/Email.py 1  DataIngestion "+CompanyNamewos+" "+entityLocation+" "+str(exc_traceback.tb_lineno)+"") 
#FOR MULTITHREADING
for j in jobs:
    j.start()
for j in jobs:
    j.join()
 
print('Stage 1 Data Ingestion end: ', datetime.datetime.now())
