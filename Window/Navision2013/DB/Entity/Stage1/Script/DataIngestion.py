from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import regexp_replace, col,udf,when,lit
import datetime
import datetime as dt
import multiprocessing
import threading, queue
from threading import Lock
import psycopg2,logging
from pyspark.sql import functions as F
import os,sys
from os.path import dirname, join, abspath
helpersDir = abspath(join(join(dirname(__file__), '..'),'..','..'))
sys.path.insert(0, helpersDir)
Abs_Path =abspath(join(join(dirname(__file__),'..'),'..'))
Kockpit_Path =abspath(join(join(dirname(__file__),'..','..','..','..')))
DBNamepath= abspath(join(join(dirname(__file__), '..'),'..','..'))
EntityNamepath=abspath(join(join(dirname(__file__), '..'),'..'))
DBName =os.path.split(DBNamepath)
EntityName =os.path.split(EntityNamepath)
DBName = DBName[1]
EntityName=EntityName[1]
entityLocation = DBName+EntityName
Stage1_Path = abspath(join(join(dirname(__file__),'..')))
STAGE1_Configurator_Path=Kockpit_Path+"/" +DBName+"/" +EntityName+"/" +"Stage1/ConfiguratorData/"
st = dt.datetime.now()
from Configuration import ModifiedTable as mt, AppConfig as ac, ModifiedModule as mm
from Configuration.Constant import *
conf = SparkConf().setMaster("local[*]").setAppName("DataIngestion").\
                    set("spark.sql.shuffle.partitions",16).\
                    set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").\
                    set("spark.local.dir", "/tmp/spark-temp").\
                    set("spark.driver.memory","30g").\
                    set("spark.executor.memory","30g").\
                    set("spark.driver.cores",'*').\
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

logging.basicConfig(filename='error.log')
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

udfBinaryToReadable = F.udf(lambda a: udfBinaryToReadableFunc(a))
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
            fullName = "parquet.`" + Kockpit_Path+"/" +DBName+"/" +EntityName+"/" +"Stage1/ParquetData"+  "/" + tblNameInHDFS + "`"
            data.append({'dbname':dirnameInHDFS, 'tblname':tblNameInHDFS, 'colname':col, 'fullname':fullName})
        SchemaDataDF=spark.createDataFrame(data, schemaForMetadata)
        SchemaDataDF.write.jdbc(url=PostgresDbInfo.url.format(PostgresDbInfo.MetadataDB), table="stage1schema", mode='append', properties=PostgresDbInfo.props)
        
    except Exception as ex:
        print("Error in updating Schema",str(ex))
        data =[]
        for col in tableColumns:
            fullName = "parquet.`" + Kockpit_Path+"/" +DBName+"/" +EntityName+"/" +"Stage1/ParquetData" +  "/" + tblNameInHDFS + "`"
            data.append({'dbname':dirnameInHDFS, 'tblname':tblNameInHDFS, 'colname':col, 'fullname':fullName})
        SchemaDataDF=spark.createDataFrame(data, schemaForMetadata)
        SchemaDataDF.write.jdbc(url=PostgresDbInfo.url.format(PostgresDbInfo.MetadataDB), table="stage1schema", mode='overwrite', properties=PostgresDbInfo.props)
        
def dataFrameWriterIntoHDFS(colSelector, tblFullName, sqlurl, hdfsLoc, dirnameInHDFS, tblNameInHDFS):
    tblQuery = "(SELECT "+ colSelector +" FROM ["+tblFullName+"]) AS FullWrite"
    if tblNameInHDFS=="G_L Entry":
        tableDataDF = spark.read.format(ConnectionInfo.JDBC_PARAM).options(url=sqlurl,dbtable=tblQuery,driver=ConnectionInfo.SQL_SERVER_DRIVER,partitionColumn="Entry No_",lowerBound=0, upperBound=30000000, numPartitions=20).load()
    if tblNameInHDFS=="Value Entry":
        tableDataDF = spark.read.format(ConnectionInfo.JDBC_PARAM).options(url=sqlurl,dbtable=tblQuery,driver=ConnectionInfo.SQL_SERVER_DRIVER,partitionColumn="Entry No_",lowerBound=0, upperBound=10000000, numPartitions=10).load()
    if tblNameInHDFS=="Item Ledger Entry":
        tableDataDF = spark.read.format(ConnectionInfo.JDBC_PARAM).options(url=sqlurl,dbtable=tblQuery,driver=ConnectionInfo.SQL_SERVER_DRIVER,partitionColumn="Entry No_",lowerBound=0, upperBound=6500000, numPartitions=10).load()
    else:
        tableDataDF = spark.read.format(ConnectionInfo.JDBC_PARAM).options(url=sqlurl,dbtable=tblQuery,driver=ConnectionInfo.SQL_SERVER_DRIVER,fetchsize=500000).load()
    
    tableDataDF = tableDataDF.select([F.col(col).alias(col.replace(' ', '').replace('(', '').replace(')', '')) for col in tableDataDF.columns])
    tableDataDF.coalesce(1).write.mode("overwrite").option("overwriteSchema", "true").save(hdfsLoc)
    updateSchemaMetadata(dirnameInHDFS, tblNameInHDFS, tableDataDF.columns)

def schemaWriterIntoHDFS(sqlurl, HDFSDatabase, SQLNAVSTRING):
    HDFS_Schema_Path =Kockpit_Path+"/" +DBName+"/" +EntityName+"/" +"Stage1/ParquetData/" + "Schema"
    fullschemaQuery = "(SELECT COLUMN_NAME,DATA_TYPE,TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WITH (NOLOCK) where TABLE_NAME like '" + SQLNAVSTRING + "%') AS FullSchema"
    fullschema = spark.read.format(ConnectionInfo.JDBC_PARAM).options(url=sqlurl,dbtable=fullschemaQuery,driver=ConnectionInfo.SQL_SERVER_DRIVER,fetchsize=10000).load()
    fullschema.coalesce(1).write.mode("overwrite").parquet(HDFS_Schema_Path)
    
def task(entity, Table, sqlurl, hdfsLocation,Schema_DF):
    table_name = entity + Table['Table']
    HDFS_Path=Kockpit_Path+"/" +DBName+"/" +EntityName+"/" +"Stage1/ParquetData/" +Table['Table']
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
                
        dataFrameWriterIntoHDFS(temp, table_name, sqlurl, HDFS_Path, hdfsLocation, Table['Table'])
        
    except Exception as ex:
        print("For Table: " + str(Table))
        exc_type,exc_value,exc_traceback=sys.exc_info()
        print('Error in Task: ', Table, ' -> ', str(ex))
        logger.endExecution()
        try:
            IDEorBatch = sys.argv[1]
        except Exception as e :
            IDEorBatch = "IDLE"
        os.system("spark-submit "+Kockpit_Path+"/Email.py 1  DataIngestion "+CompanyNamewos+" "+entityLocation+" "+str(exc_traceback.tb_lineno)+"") 
        log_dict = logger.getErrorLoggedRecord('DataIngestion'+table_name, '', '', str(ex), exc_traceback.tb_lineno, IDEorBatch)
        log_df = spark.createDataFrame(log_dict, logger.getSchema())
        log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props) 
jobs = []
moduleName = sys.argv[1] if len(sys.argv) > 1 else ''

def Configurator(ConfTab, STAGE1_Configurator_Path):
    Query="(SELECT *\
                FROM "+ConfiguratorDbInfo.Schema+"."+chr(34)+ConfTab+chr(34)+") AS df"
    Configurator_Data = spark.read.format("jdbc").options(url=ConfiguratorDbInfo.PostgresUrl, dbtable=Query,user=ConfiguratorDbInfo.props["user"],password=ConfiguratorDbInfo.props["password"],driver= ConfiguratorDbInfo.props["driver"]).load()
    Configurator_Data = Configurator_Data.select([F.col(col).alias(col.replace(" ","")) for col in Configurator_Data.columns])
    Configurator_Data = Configurator_Data.select([F.col(col).alias(col.replace("(","")) for col in Configurator_Data.columns])
    Configurator_Data = Configurator_Data.select([F.col(col).alias(col.replace(")","")) for col in Configurator_Data.columns])
    Configurator_Data.coalesce(1).write.mode("overwrite").parquet(STAGE1_Configurator_Path+ConfTab)
  
table_names = spark.read.format("jdbc").options(url=ConfiguratorDbInfo.PostgresUrl,dbtable='information_schema.tables',user=ConfiguratorDbInfo.props["user"],password=ConfiguratorDbInfo.props["password"],driver=ConfiguratorDbInfo.props["driver"]).load().\
filter("table_schema = '"+ConfiguratorDbInfo.Schema+"'").select("table_name")
table_names_list = [row.table_name for row in table_names.collect()]
for ConfTab in table_names_list:
    try:                         
        t1 = threading.Thread(target=Configurator, args=(ConfTab, STAGE1_Configurator_Path))
        jobs.append(t1)
    except Exception as ex:
        exc_type,exc_value,exc_traceback=sys.exc_info()
        print(str(ex))
        print("type - "+str(exc_type))
        print("File - "+exc_traceback.tb_frame.f_code.co_filename)
        print("Error Line No. - "+str(exc_traceback.tb_lineno))
       
for entityObj in ac.config["DbEntities"]:
    entity = entityObj["Name"]
    entityLocation = entityObj["Location"]
    dbName = entityObj["DatabaseName"]
    hdfspath = Kockpit_Path+"/" +DBName+"/" +EntityName
    sqlurl=ConnectionInfo.SQL_URL.format(ac.config["SourceDBConnection"]['url'], str(ac.config["SourceDBConnection"]["port"]), dbName, ac.config["SourceDBConnection"]['userName'],ac.config["SourceDBConnection"]['password'],ac.config["SourceDBConnection"]['dbtable'])
    schemaWriterIntoHDFS(sqlurl, entityLocation, entity)
    HDFS_Schema_DF = spark.read.parquet(Kockpit_Path+"/" +DBName+"/" +EntityName+"/" +"Stage1/ParquetData" + "/Schema")
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
            print("Block 2")
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

# for i in chunks(jobs,10):
#     for j in i:
#         j.start()
#     for j in i:
#         j.join()
# # FOR MULTITHREADING
for j in jobs:
    j.start()
for j in jobs:
    j.join()
print('Stage 1 DataIngestion end: ', datetime.datetime.now())
