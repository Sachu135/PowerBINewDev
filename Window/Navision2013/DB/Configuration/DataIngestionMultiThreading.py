from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession,Row
#from pyspark.sql.functions import *
from pyspark.sql import functions as f
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import regexp_replace, col, udf, broadcast
import datetime
import datetime as dt
import multiprocessing
import threading, queue
from threading import Lock
from multiprocessing.pool import ThreadPool
import psycopg2,findspark

from pyspark.sql import functions as F
import pandas as pd
import os,sys,subprocess
from os.path import dirname, join, abspath

helpersDir = abspath(join(dirname(__file__), '..'))
sys.path.insert(0, helpersDir)
from ConfigurationFiles.AppConfig import *
from Helpers.Constants import *

#spark://192.10.15.132:7077
print('Stage 1 Data Ingestion start: ', datetime.datetime.now())

conf = SparkConf().setMaster('spark://192.10.15.132:7077').setAppName("Stage1:DataIngestion")\
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
                .set("spark.kryoserializer.buffer.max","512m")\
                .set("spark.executor.memory","8g")\
                .set("spark.driver.memory","24g")\
                .set("spark.driver.maxResultSize","20g")\
                .set("spark.memory.offHeap.enabled",'true')\
                .set("spark.memory.offHeap.size","100g")\
                .set('spark.scheduler.mode', 'FAIR')\
                .set("spark.sql.broadcastTimeout", "36000")\
                .set("spark.network.timeout", 10000000)\
                .set("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0")\
                .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
                .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
                .set("spark.rapids.sql.enabled", True)\
                .set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
                
'''
.set("spark.executor.cores","3")\
.set("spark.executor.memory","4g")\
'''
sc = SparkContext(conf = conf)
sqlCtx = SQLContext(sc)
spark = sqlCtx.sparkSession

import delta
from delta.tables import *

#Connection string of SQL Database 

#sqlurl=ConnectionInfo.SQL_URL.format(config["SourceDBConnection"]['url'], str(config["SourceDBConnection"]["port"]), config["SourceDBConnection"]['databaseName'], config["SourceDBConnection"]['userName'], config["SourceDBConnection"]['password'],config["SourceDBConnection"]['dbtable'])

numberOfThreads = 8
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
    delQuery = 'DELETE FROM stage1schema where "dbname" = \'' + dirnameInHDFS + '\' and "tblname" = \'' + tblNameInHDFS + '\''
    con = psycopg2.connect(database=PostgresDbInfo.MetadataDB, user=PostgresDbInfo.props["user"], password=PostgresDbInfo.props["password"], host=PostgresDbInfo.Host, port="5432")
    cur = con.cursor()
    cur.execute(delQuery)
    con.commit()
    con.close()

    data =[]
    for col in tableColumns:
        #fullName = "delta.`hdfs://hadoopmaster:9000/DataMart/Stage1/DB1E1/Courier Zone`"
        fullName = "delta.`" + STAGE1_PATH + "/" + dirnameInHDFS + "/" + tblNameInHDFS + "`"
        data.append({'dbname':dirnameInHDFS, 'tblname':tblNameInHDFS, 'colname':col, 'fullname':fullName})
    
    SchemaDataDF=spark.createDataFrame(data, schemaForMetadata)
    SchemaDataDF.write.jdbc(url=PostgresDbInfo.url.format(PostgresDbInfo.MetadataDB), table="stage1schema", mode='append', properties=PostgresDbInfo.props)
    print(tblNameInHDFS + ' Schema saved in .metadata database')

def dataFrameWriterIntoHDFS(colSelector, tblFullName, sqlurl, hdfsLoc, dirnameInHDFS, tblNameInHDFS):
    print(tblFullName + ': Overwrite')
    tblQuery = "(SELECT "+ colSelector +" FROM ["+tblFullName+"]) AS data1"
    tableDataDF = spark.read.format(ConnectionInfo.JDBC_PARAM).options(url=sqlurl,dbtable=tblQuery,driver=ConnectionInfo.SQL_SERVER_DRIVER,fetchsize=10000).load() #numPartitions="200"
    tableDataDF = tableDataDF.select([F.col(col).alias(col.replace(' ', '').replace('(', '').replace(')', '')) for col in tableDataDF.columns])
    print("Writing Data")
    tableDataDF.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(hdfsLoc)
    
    print(tblFullName + ' Data dumped into HDFS')
    updateSchemaMetadata(dirnameInHDFS, tblNameInHDFS, tableDataDF.columns)
    
    

def task(entity, Table, sqlurl, hdfsLocation):
    table_name = entity + Table['Table']
    HDFS_Path = STAGE1_PATH + "/" + hdfsLocation + "/" + Table['Table']
    query= '(SELECT * from [dbo].['+table_name+']) as q1'
    try:
        print('Process for table started: ' + table_name)
        st = dt.datetime.now()
        schemaQuery = "(SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS where TABLE_NAME="+chr(39)+table_name+chr(39)+") AS data"
        
        schema = spark.read.format(ConnectionInfo.JDBC_PARAM).options(url=sqlurl,dbtable=schemaQuery,driver=ConnectionInfo.SQL_SERVER_DRIVER,fetchsize=10000).load().collect() #numPartitions="200"
        
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
  
            if schema[i]['DATA_TYPE'] =='sql_variant':
                temp = temp +",CONVERT(varchar,"+col_name_temp+",20) as "+col_name_temp
            elif incrementalLoadColumn == schema[i]['COLUMN_NAME']:
                temp = temp + (',' if i > 0 else '') + 'CAST([' + schema[i]['COLUMN_NAME'] + '] AS BIGINT) AS [' + schema[i]['COLUMN_NAME'] + ']'
            else:
                temp = temp + col_name_temp
 
        tableQuery = "(SELECT "+temp+" FROM ["+table_name+"] ) AS data1" #WHERE 1=0
        #print(tableQuery)
        #schemaDF = spark.read.format(ConnectionInfo.JDBC_PARAM).options(url=sqlurl,dbtable=tableQuery,driver=ConnectionInfo.SQL_SERVER_DRIVER,fetchsize=10000).load()
        
        schemaDF = spark.read.format(ConnectionInfo.JDBC_PARAM).options(url=sqlurl,dbtable=tableQuery,driver=ConnectionInfo.SQL_SERVER_DRIVER,fetchsize=10000).load()
        
        schemaDF = schemaDF.select([F.col(col).alias(col.replace(' ', '').replace('(', '').replace(')', '')) for col in schemaDF.columns])
        
        schemaJoiner = [x+'#'+y for x,y in schemaDF.dtypes]
        #print(schemaJoiner)
        schemaStr = "|".join(schemaJoiner)
        #print(schemaStr)
        # exit()
        try:
            hdfsDF = None
            
            if 'Key' in Table and 'CheckOn' in Table:
                #Incremental load is possible
                #Check if file exists in HDFS
                try:
                    hdfsDF = spark.read.format("delta").load(HDFS_Path) #spark.read.parquet(HDFS_Path)
                except Exception as e:
                    #Save table data into HDFS directly if no file exists in HDFS
                    print(table_name + ": Table is not present in HDFS, So creating in HDFS")
                    dataFrameWriterIntoHDFS(temp, table_name, sqlurl, HDFS_Path, hdfsLocation, Table['Table'])
                    #exit()
                else:
                    hdfsSchemaJoiner = [x+'#'+y for x,y in hdfsDF.dtypes]
                   
                    hdfsSchemaStr = "|".join(hdfsSchemaJoiner)
                    
                    if(schemaStr == hdfsSchemaStr):
                        #Schema matched with Source table, incremental load required
                        print(table_name + ': Schema matched')
                        maxVal = hdfsDF.agg({Table['CheckOn']: "max"}).collect()[0][0]
                        
                    
                        print(table_name + ": Max Timestamp: " ,str(maxVal))
                        
                        if maxVal is None:
                            dataFrameWriterIntoHDFS(temp, table_name, sqlurl, HDFS_Path, hdfsLocation, Table['Table'])
                            return
                            
                        # Get Updated/Newly inserted records
                        tableQuery = "(SELECT * FROM (SELECT "+temp+" FROM ["+table_name+"]) AS data1 WHERE [" + Table['CheckOn'] + "] > " + str(maxVal) + ") AS data"
                        tableDataDF = spark.read.format(ConnectionInfo.JDBC_PARAM).options(url=sqlurl,dbtable=tableQuery,driver=ConnectionInfo.SQL_SERVER_DRIVER,fetchsize=10000).load()
                        tableDataDF = tableDataDF.select([F.col(col).alias(col.replace(' ', '').replace('(', '').replace(')', '')) for col in tableDataDF.columns])
                        #tableDataDF.show()
                        
                        print(table_name + ": New/Update records " + str(tableDataDF.count()))
                        
                        #Do UpSert process
                        dtTable=DeltaTable.forPath(spark, HDFS_Path)
                       
                        
                        print(table_name + ": hdfs saved records " + str(hdfsDF.count()))
                        mergeConditionsList = []
                        keyNames = []
                        modifiedKeyNames = []
                        for key in Table['Key']:
                            mergeConditionsList.append("(source." + key.replace(' ', '').replace('(', '').replace(')', '') + " = dest." + key.replace(' ', '').replace('(', '').replace(')', '') + ")")
                            keyNames.append(("[" + key + "]"))
                            modifiedKeyNames.append(key.replace(' ', '').replace('(', '').replace(')', ''))
                              
                        mergeCondition = ("" + " and ".join(mergeConditionsList) + "") if len(mergeConditionsList) > 1 else "".join(mergeConditionsList) 
                        print(mergeCondition)
                        exit()
                        #hdfsDF.printSchema()
                        #tableDataDF.printSchema()
                        if tableDataDF.count() > 0:
                            dtTable.alias("source").merge(tableDataDF.alias("dest"), mergeCondition).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()                            
                            updateSchemaMetadata(hdfsLocation, Table['Table'], tableDataDF.columns)
                            print(table_name + ': UpSert done')
                        else:
                            print(table_name+ ': No UpSert required')   
                        hdfsDF = spark.read.format("delta").load(HDFS_Path)
                        print(table_name + ": After UpSert hdfs total records " + str(hdfsDF.count()))
                        
                        #Delete records from hdfs df if not available in source
                        #print(keyNames)
                        tempForKeys = ''
                        for i in range(0,len(schema)):
                            col_name_temp = "["+schema[i]['COLUMN_NAME']+"]"
                            if col_name_temp in keyNames:
                                if schema[i]['DATA_TYPE'] =='sql_variant':
                                    tempForKeys = tempForKeys + (' , ' if len(tempForKeys) > 0 else '') + "CONVERT(varchar,"+col_name_temp+",20) as "+col_name_temp
                                else:
                                    tempForKeys = tempForKeys + (' , ' if len(tempForKeys) > 0 else '') + col_name_temp

                        tableQuery = "(SELECT " + tempForKeys + " FROM ["+table_name+"]) AS data1"
                        #print(tableQuery)
                        onlyIDsDF = spark.read.format(ConnectionInfo.JDBC_PARAM).options(url=sqlurl,dbtable=tableQuery,driver=ConnectionInfo.SQL_SERVER_DRIVER,fetchsize=10000).load()
                        onlyIDsDF = onlyIDsDF.select([F.col(col).alias(col.replace(' ', '').replace('(', '').replace(')', '')) for col in onlyIDsDF.columns])                         
                        
                        print(table_name + ": Source Total records " + str(onlyIDsDF.count()))
                        #print(modifiedKeyNames)
                        #hdfsDF.printSchema()
                        #onlyIDsDF.printSchema()
                        recordsToBeDeleteDF = hdfsDF.alias("a").join(onlyIDsDF.alias("b"), modifiedKeyNames, 'left').where(onlyIDsDF[onlyIDsDF.columns[0]].isNull()).selectExpr("a.*")
                        recordsToBeDeleteDFCount = recordsToBeDeleteDF.count()
                        print(table_name + ": Records need to be deleted " + str(recordsToBeDeleteDFCount))
                        
                        mergeConditionsList.append("(source." + incrementalLoadColumn.replace(' ', '').replace('(', '').replace(')', '') + " = dest." + incrementalLoadColumn.replace(' ', '').replace('(', '').replace(')', '') + ")")
 
                        mergeConditionForDeletion = ("" + " and ".join(mergeConditionsList) + "") if len(mergeConditionsList) > 1 else "".join(mergeConditionsList) 
                           
                        #print(mergeConditionForDeletion)
                        if recordsToBeDeleteDFCount > 0:
                            hdfsDF = DeltaTable.forPath(spark, HDFS_Path);
                            hdfsDF.alias("source").merge(recordsToBeDeleteDF.alias("dest"), mergeConditionForDeletion).whenMatchedDelete().execute();
                        
                        #dtTable.show(1)
                        #print("After deletion records " + str(dtTable.count()))
                        hdfsDF = spark.read.format("delta").load(HDFS_Path) 
                        print(table_name + ": After deletion HDFS total records " + str(hdfsDF.count()))
                        print(table_name + ': extra records deleted on destination(HDFS) if any')
                    else:
                        #Full reload required
                        dataFrameWriterIntoHDFS(temp, table_name, sqlurl, HDFS_Path, hdfsLocation, Table['Table'])
                        print(table_name + ': schema not matched')
            else:
                #Only Full reload possible
                dataFrameWriterIntoHDFS(temp, table_name, sqlurl, HDFS_Path, hdfsLocation, Table['Table'])
            #exit()
            
        except Exception as ex:
            print("For Table: " + str(Table))
            exc_type,exc_value,exc_traceback=sys.exc_info()
            print("Error: " + table_name, ex)
            print("type - " + str(exc_type))
            print("File - " + exc_traceback.tb_frame.f_code.co_filename)
            print("Error Line No. - " + table_name + " " + str(exc_traceback.tb_lineno))
            #print(e)
            #continue          
        
        ts = (dt.datetime.now()-st).total_seconds()
        print(table_name + ': Process for table ended (seconds): ' + str(ts))
    except Exception as ex:
        print('Error in Task: ', Table, ' -> ', str(ex))
    
jobs = []
moduleName = sys.argv[1] if len(sys.argv) > 1 else ''
print("2")
print(moduleName)
print("1")
exit()

# print(Modules)

# print(Table['Modules'])
# print(Table)
# print(table_name)

   

for entityObj in config["DbEntities"]:
    entity = entityObj["Name"]
    entityLocation = entityObj["Location"]
    dbName = entityObj["DatabaseName"]
    sqlurl=ConnectionInfo.SQL_URL.format(config["SourceDBConnection"]['url'], str(config["SourceDBConnection"]["port"]), dbName, config["SourceDBConnection"]['userName'], config["SourceDBConnection"]['password'],config["SourceDBConnection"]['dbtable'])
    query = "(SELECT name from sys.tables where name like '" + entity + "%') as data"
    
    
    viewDF = spark.read.format(ConnectionInfo.JDBC_PARAM).options(url=sqlurl,dbtable=query,driver=ConnectionInfo.SQL_SERVER_DRIVER,fetchsize=10000).load()
    list_of_tables = viewDF.select(viewDF.name).rdd.flatMap(lambda x: x).collect()
    # print(list_of_tables)

    for Table in config["TablesToIngest"]:
        try:
            
            
            table_name = entity + Table['Table']
            #print(table_name, 'Modules' in Table)
            #list_of_tables = ['Team Computers Pvt Ltd$Value Entry']
            #print(list_of_tables)
            
            if (len(moduleName) == 0 and table_name in list_of_tables) \
                or (len(moduleName) > 0 and 'Modules' in Table and moduleName in Table['Modules'] and table_name in list_of_tables) \
                or (len(moduleName) > 0 and 'Modules' not in Table and table_name in list_of_tables):
                #print(table_name)
                
                
                try:
                    #For batch job 1 by 1
                    #task(entity, Table)                    
                    t = threading.Thread(target=task, args=(entity, Table, sqlurl, entityLocation))
                    jobs.append(t)
                except Exception as ex:
                    exc_type,exc_value,exc_traceback=sys.exc_info()
                    print(str(ex))
                    print("type - "+str(exc_type))
                    print("File - "+exc_traceback.tb_frame.f_code.co_filename)
                    print("Error Line No. - "+str(exc_traceback.tb_lineno)) 
            else:
                print(table_name+' table name does not exist in SQL Database')
                print(len(moduleName))
                print(moduleName)
                
        except Exception as ex:
            print("Block 2")
            exc_type,exc_value,exc_traceback=sys.exc_info()
            print("Error:",ex)
            print("type - "+str(exc_type))
            print("File - "+exc_traceback.tb_frame.f_code.co_filename)
            print("Error Line No. - "+str(exc_traceback.tb_lineno))
        
    #spark.catalog.clearCache()

print('jobs count', len(jobs))

for j in jobs:
    j.start()
for j in jobs:
    j.join()
    
print('Stage 1 Data Ingestion end: ', datetime.datetime.now())
