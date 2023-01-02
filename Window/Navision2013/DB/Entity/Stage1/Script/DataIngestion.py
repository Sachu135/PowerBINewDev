from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import regexp_replace, col, udf, when, lit
import datetime
import datetime as dt
import multiprocessing
import threading, queue
from threading import Lock
import psycopg2, logging
from pyspark.sql import functions as F
import os, sys
from os.path import dirname, join, abspath

Abs_Path = abspath(join(join(dirname(__file__), '..'), '..'))
Kockpit_Path = abspath(join(join(dirname(__file__), '..', '..', '..', '..')))
DBNamepath = abspath(join(join(dirname(__file__), '..'), '..', '..'))
EntityNamepath = abspath(join(join(dirname(__file__), '..'), '..'))
sys.path.insert(0, DBNamepath)
sys.path.insert(0, EntityNamepath)
DBName = os.path.split(DBNamepath)
EntityName = os.path.split(EntityNamepath)

DBName = DBName[1]
EntityName = EntityName[1]
ENTITY_LOCATION_LOCAL = DBName + EntityName
entityLocation = DBName + EntityName
Stage1_Path = abspath(join(join(dirname(__file__), '..')))
STAGE1_Configurator_Path = Stage1_Path + "/ConfiguratorData/"
st = dt.datetime.now()
from Configuration import AppConfig as ac
from Configuration.udf import *
from Configuration.Constant import *

sqlCtx, spark = getSparkConfig("local[*]", "Stage1:DataIngestion")
logging.basicConfig(filename='error.log')
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
    StructField("dbname", StringType(), False),
    StructField("tblname", StringType(), False),
    StructField("colname", StringType(), False),
    StructField("fullname", StringType(), False)
])


def dataFrameWriterIntoHDFS(colSelector, tblFullName, sqlurl, hdfsLoc, dirnameInHDFS, tblNameInHDFS):
    tblQuery = "(SELECT " + colSelector + " FROM [" + tblFullName + "]) AS FullWrite"
    tableDataDF = spark.read.format(ConnectionInfo.JDBC_PARAM).options(url=sqlurl, dbtable=tblQuery,
                                                                       driver=ConnectionInfo.SQL_SERVER_DRIVER,
                                                                       fetchsize=100000,batchsize=100000).load()

    tableDataDF = tableDataDF.select(
        [F.col(col).alias(col.replace(' ', '').replace('(', '').replace(')', '')) for col in tableDataDF.columns])
    tableDataDF.write.option("maxRecordsPerFile", 20000).mode("overwrite").option("overwriteSchema", "true").save(hdfsLoc)


def schemaWriterIntoHDFS(sqlurl, HDFSDatabase, SQLNAVSTRING):
    fullschemaQuery = "(SELECT COLUMN_NAME,DATA_TYPE,TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WITH (NOLOCK) where TABLE_NAME like '" + SQLNAVSTRING + "%') AS FullSchema"
    fullschema = spark.read.format(ConnectionInfo.JDBC_PARAM).options(url=sqlurl, dbtable=fullschemaQuery,
                                                                      driver=ConnectionInfo.SQL_SERVER_DRIVER,
                                                                      fetchsize=10000).load()
    return fullschema


def task(entity, Table, sqlurl, hdfsLocation, Schema_DF):
    table_name = entity + Table['Table']
    HDFS_Path = Kockpit_Path + "/" + hdfsLocation + "/Stage1/ParquetData/" + Table['Table']
    try:
        logger = Logger()
        st = dt.datetime.now()
        schema = Schema_DF.filter(Schema_DF['TABLE_NAME'] == table_name).select('COLUMN_NAME', 'DATA_TYPE')
        schema = schema.filter(schema['COLUMN_NAME'].isin(Table['Columns']))
        schema_Columns = schema.withColumn('COLUMN_NAME', regexp_replace(schema['COLUMN_NAME'], "[( )]", ""))
        schema_Columns = schema_Columns.withColumn('DATA_TYPE', when(
            schema_Columns['DATA_TYPE'].isin(['nvarchar', 'varchar', 'sql_variant']), lit('string')) \
                                                   .when(schema_Columns['DATA_TYPE'] == 'decimal',
                                                         lit('decimal(38,20)')) \
                                                   .when(schema_Columns['DATA_TYPE'] == 'tinyint', lit('int')) \
                                                   .when(schema_Columns['DATA_TYPE'] == 'datetime', lit('timestamp')) \
                                                   .when(schema_Columns['DATA_TYPE'].isin(['image', 'varbinary']),
                                                         lit('binary')) \
                                                   .when(schema_Columns['DATA_TYPE'] == 'uniqueidentifier',
                                                         lit('string')) \
                                                   .otherwise(schema_Columns['DATA_TYPE']))
        schema_Columns = schema_Columns.withColumn('DATA_TYPE',
                                                   when(schema_Columns['COLUMN_NAME'] == 'timestamp', lit('bigint')) \
                                                   .otherwise(schema_Columns['DATA_TYPE']))
        schema = schema.collect()
        schemaJoiner = [row.COLUMN_NAME + "#" + row.DATA_TYPE for row in schema_Columns.collect()]

        incrementalLoadColumn = ''
        if 'CheckOn' in Table:
            incrementalLoadColumn = Table['CheckOn']
        temp = ''
        for i in range(0, len(schema)):
            col_name_temp = "[" + schema[i]['COLUMN_NAME'] + "]"
            if (i == 0 or schema[i]['DATA_TYPE'] == 'sql_variant'):
                col_name_temp = col_name_temp
            elif (schema[i]['DATA_TYPE'] != 'sql_variant'):
                col_name_temp = "," + col_name_temp

            if (i == 0 and schema[i]['DATA_TYPE'] == 'sql_variant'):

                temp = temp + "CONVERT(varchar," + col_name_temp + ",20) as " + col_name_temp
            elif schema[i]['DATA_TYPE'] == 'sql_variant':

                temp = temp + ",CONVERT(varchar," + col_name_temp + ",20) as " + col_name_temp
            elif incrementalLoadColumn == schema[i]['COLUMN_NAME']:
                temp = temp + (',' if i > 0 else '') + 'CAST([' + schema[i]['COLUMN_NAME'] + '] AS BIGINT) AS [' + \
                       schema[i]['COLUMN_NAME'] + ']'
            else:
                temp = temp + col_name_temp

        dataFrameWriterIntoHDFS(temp, table_name, sqlurl, HDFS_Path, hdfsLocation, Table['Table'])

    except Exception as ex:
        print("For Table: " + str(Table))
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print('Error in Task: ', Table, ' -> ', str(ex))
        logger.endExecution()
        try:
            IDEorBatch = sys.argv[2]
        except Exception as e:
            IDEorBatch = "IDLE"
        os.system(
            "spark-submit " + Kockpit_Path + "/Email.py 1  DataIngestion " + CompanyNamewos + " " + entityLocation + " " + str(
                exc_traceback.tb_lineno) + "")
        log_dict = logger.getErrorLoggedRecord('DataIngestion' + table_name, DBName, EntityName, str(ex),
                                               exc_traceback.tb_lineno, IDEorBatch)
        log_df = spark.createDataFrame(log_dict, logger.getSchema())
        log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append',
                          properties=PostgresDbInfo.props)


jobs = []
moduleName = sys.argv[2] if len(sys.argv) > 2 else ''


def full_reload():
    for entityObj in ac.config["DbEntities"]:
        entity = entityObj["Name"]
        entityLoc = entityObj["Location"]
        DB_EN_list = entityLoc.split('E')
        DB = DB_EN_list[0]
        EntityName = 'E' + DB_EN_list[1]
        entityLocation = DB + "/" + EntityName
        dbName = entityObj["DatabaseName"]

        if entityLoc == ENTITY_LOCATION_LOCAL:

            STAGE1_PATH = Kockpit_Path + "/" + DBName + "/" + EntityName + "/Stage1"
            sqlurl = ConnectionInfo.SQL_URL.format(ac.config["SourceDBConnection"]['url'],
                                                   str(ac.config["SourceDBConnection"]["port"]), dbName,
                                                   ac.config["SourceDBConnection"]['userName'],
                                                   ac.config["SourceDBConnection"]['password'],
                                                   ac.config["SourceDBConnection"]['dbtable'])
            HDFS_Schema_DF = schemaWriterIntoHDFS(sqlurl, entityLocation, entity)
            query = "(SELECT name from sys.tables where name like '" + entity + "%') as data"
            viewDF = spark.read.format(ConnectionInfo.JDBC_PARAM).options(url=sqlurl, dbtable=query,
                                                                          driver=ConnectionInfo.SQL_SERVER_DRIVER,
                                                                          fetchsize=10000).load()
            list_of_tables = viewDF.select(viewDF.name).rdd.flatMap(lambda x: x).collect()
            for Table in ac.config["TablesToIngest"]:
                try:
                    logger = Logger()
                    table_name = entity + Table['Table']
                    CompanyNamewos = Table['Table'].replace(" ", "")

                    if (len(moduleName) == 0 and table_name in list_of_tables) \
                            or (len(moduleName) > 0 and 'Modules' in Table and moduleName in Table[
                        'Modules'] and table_name in list_of_tables) \
                            or (len(moduleName) > 0 and 'Modules' not in Table and table_name in list_of_tables):

                        try:
                            t = threading.Thread(target=task,
                                                 args=(entity, Table, sqlurl, entityLocation, HDFS_Schema_DF))
                            jobs.append(t)
                            logger.endExecution()
                            try:
                                IDEorBatch = sys.argv[2]
                            except Exception as e:
                                IDEorBatch = "IDLE"

                            log_dict = logger.getSuccessLoggedRecord("DataIngestion_" + table_name, DBName, EntityName,
                                                                     0, 0, IDEorBatch)
                            log_df = spark.createDataFrame(log_dict, logger.getSchema())
                            log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append',
                                              properties=PostgresDbInfo.props)
                        except Exception as ex:
                            exc_type, exc_value, exc_traceback = sys.exc_info()
                            print(str(ex))
                            print("type - " + str(exc_type))
                            print("File - " + exc_traceback.tb_frame.f_code.co_filename)
                            print("Error Line No. - " + str(exc_traceback.tb_lineno))
                            ex = str(ex)
                            logger.endExecution()
                            try:
                                IDEorBatch = sys.argv[2]
                            except Exception as e:
                                IDEorBatch = "IDLE"
                            os.system(
                                "spark-submit " + Kockpit_Path + "/Email.py 1  DataIngestion " + CompanyNamewos + " " + entityLocation + " " + str(
                                    exc_traceback.tb_lineno) + "")
                            log_dict = logger.getErrorLoggedRecord('DataIngestion' + table_name, DBName, EntityName,
                                                                   str(ex), exc_traceback.tb_lineno, IDEorBatch)
                            log_df = spark.createDataFrame(log_dict, logger.getSchema())
                            log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append',
                                              properties=PostgresDbInfo.props)
                    else:
                        print(table_name + ' table name does not exist in SQL Database')

                except Exception as ex:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    print("Error:", ex)
                    print("type - " + str(exc_type))
                    print("File - " + exc_traceback.tb_frame.f_code.co_filename)
                    print("Error Line No. - " + str(exc_traceback.tb_lineno))
                    ex = str(ex)
                    logger.endExecution()
                    try:
                        IDEorBatch = sys.argv[2]
                    except Exception as e:
                        IDEorBatch = "IDLE"
                    os.system(
                        "spark-submit " + Kockpit_Path + "/Email.py 1  DataIngestion " + CompanyNamewos + " " + entityLocation + " " + str(
                            exc_traceback.tb_lineno) + "")
                    log_dict = logger.getErrorLoggedRecord('DataIngestion', DBName, EntityName, str(ex),
                                                           exc_traceback.tb_lineno, IDEorBatch)
                    log_df = spark.createDataFrame(log_dict, logger.getSchema())
                    log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append',
                                      properties=PostgresDbInfo.props)


def Configurator(ConfTab, STAGE1_Configurator_Path, DB, EntityName):
    try:
        logger=Logger() 
        CompanyNamewos=ConfTab.replace(" ","")
        Query = "(SELECT *\
                    FROM " + ConfiguratorDbInfo.Schema + "." + chr(34) + ConfTab + chr(34) + ") AS df"
        Configurator_Data = spark.read.format("jdbc").options(url=ConfiguratorDbInfo.PostgresUrl, dbtable=Query,
                                                              user=ConfiguratorDbInfo.props["user"],
                                                              password=ConfiguratorDbInfo.props["password"],
                                                              driver=ConfiguratorDbInfo.props["driver"]).load()
    
        Configurator_Data = Configurator_Data.select(
            [F.col(col).alias(col.replace(" ", "")) for col in Configurator_Data.columns])
        Configurator_Data = Configurator_Data.select(
            [F.col(col).alias(col.replace("(", "")) for col in Configurator_Data.columns])
        Configurator_Data = Configurator_Data.select(
            [F.col(col).alias(col.replace(")", "")) for col in Configurator_Data.columns])
        if all(x in Configurator_Data.columns for x in ['DBName', 'EntityName']):
            Configurator_Data = Configurator_Data.filter(
                (Configurator_Data['DBName'] == DB) & (Configurator_Data['EntityName'] == EntityName))
    
        Configurator_Data.write.format("parquet").mode("overwrite").option("overwriteSchema", "true").save(
            STAGE1_Configurator_Path + ConfTab)
    
    except Exception as ex:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    print("Error:", ex)
                    print("type - " + str(exc_type))
                    print("File - " + exc_traceback.tb_frame.f_code.co_filename)
                    print("Error Line No. - " + str(exc_traceback.tb_lineno))
                    ex = str(ex)
                    logger.endExecution()
                    try:
                        IDEorBatch = sys.argv[2]
                    except Exception as e:
                        IDEorBatch = "IDLE"
                    os.system(
                        "spark-submit " + Kockpit_Path + "/Email.py 1  DataIngestion " + CompanyNamewos + " " + entityLocation + " " + str(
                            exc_traceback.tb_lineno) + "")
                    log_dict = logger.getErrorLoggedRecord('DataIngestion', DBName, EntityName, str(ex),
                                                           exc_traceback.tb_lineno, IDEorBatch)
                    log_df = spark.createDataFrame(log_dict, logger.getSchema())
                    log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append',
                                      properties=PostgresDbInfo.props)

def Config_reload():
    table_names = spark.read.format("jdbc").options(url=ConfiguratorDbInfo.PostgresUrl,
                                                    dbtable='information_schema.tables',
                                                    user=ConfiguratorDbInfo.props["user"],
                                                    password=ConfiguratorDbInfo.props["password"],
                                                    driver=ConfiguratorDbInfo.props["driver"]).load(). \
        filter("table_schema = '" + ConfiguratorDbInfo.Schema + "'").select("table_name")
    table_names_list = [row.table_name for row in table_names.collect()]
    for entityObj in ac.config["DbEntities"]:
        entity = entityObj["Name"]
        entityLoc = entityObj["Location"]
        DB_EN_list = entityLoc.split('E')
        DB = DB_EN_list[0]
        EntityName = 'E' + DB_EN_list[1]
        entityLocation = DB + "/" + EntityName
        dbName = entityObj["DatabaseName"]
        STAGE1_PATH = Kockpit_Path + "/" + DBName + "/" + EntityName + "/Stage1"
        STAGE1_Configurator_Path = STAGE1_PATH + "/ConfiguratorData/"
        if entityLoc == ENTITY_LOCATION_LOCAL:
            for ConfTab in table_names_list:
                try:
                    CompanyNamewos = ConfTab.replace(" ", "")
                    logger = Logger()
                    t1 = threading.Thread(target=Configurator, args=(ConfTab, STAGE1_Configurator_Path, DB, EntityName))
                    jobs.append(t1)
                    logger.endExecution()
                    try:
                        IDEorBatch = sys.argv[2]
                    except Exception as e:
                        IDEorBatch = "IDLE"

                    log_dict = logger.getSuccessLoggedRecord("DataIngestion_" + ConfTab, DBName, EntityName, 0, 0,
                                                             IDEorBatch)
                    log_df = spark.createDataFrame(log_dict, logger.getSchema())
                    log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append',
                                      properties=PostgresDbInfo.props)
                except Exception as ex:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    print(str(ex))
                    print("type - " + str(exc_type))
                    print("File - " + exc_traceback.tb_frame.f_code.co_filename)
                    print("Error Line No. - " + str(exc_traceback.tb_lineno))
                    ex = str(ex)
                    logger.endExecution()
                    try:
                        IDEorBatch = sys.argv[2]
                    except Exception as e:
                        IDEorBatch = "IDLE"
                    os.system(
                        "spark-submit " + Kockpit_Path + "/Email.py 1  DataIngestion " + CompanyNamewos + " " + entityLocation + " " + str(
                            exc_traceback.tb_lineno) + "")
                    log_dict = logger.getErrorLoggedRecord('DataIngestion' + ConfTab, DBName, EntityName, str(ex),
                                                           exc_traceback.tb_lineno, IDEorBatch)
                    log_df = spark.createDataFrame(log_dict, logger.getSchema())
                    log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append',
                                      properties=PostgresDbInfo.props)


def Reload(Type):
    if Type == 'FULL':
        full_reload()
    elif Type == 'CONFIGURATOR':
        Config_reload()
    else:
        full_reload()
        Config_reload()
    print(jobs)
    for j in jobs:
        j.start()
    for j in jobs:
        j.join()
    print('Stage 1 Full_reload end: ', datetime.datetime.now())


if __name__ == "__main__":
    Reload('')
