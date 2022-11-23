from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import *
import io,os,datetime
from pyspark.sql.functions import col,concat,when,lit,udf,length,explode,split,regexp_replace,coalesce
import datetime,time,sys
from pyspark.sql.types import *
from pyspark.sql.types import StringType
from builtins import str
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
entityLocation = DBName+EntityName
STAGE1_Configurator_Path=Kockpit_Path+"/" +DBName+"/" +EntityName+"/" +"Stage1/ConfiguratorData/"
STAGE1_PATH=Kockpit_Path+"/" +DBName+"/" +EntityName+"/" +"Stage1/ParquetData"
STAGE2_PATH=Kockpit_Path+"/" +DBName+"/" +EntityName+"/" +"Stage2/ParquetData"
conf = SparkConf().setMaster("local[*]").setAppName("MISPNL").\
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
for dbe in config["DbEntities"]:
    if dbe['ActiveInactive']=='true' and  dbe['Location']==DBEntity:
        CompanyName=dbe['Name']
        CompanyName=CompanyName.replace(" ","")
        try:
            logger = Logger()
            Acc_Sche =spark.read.format("parquet").load(STAGE1_PATH+"/Acc_ Schedule Line")
            COA = spark.read.format("parquet").load(STAGE1_PATH+"/G_L Account")
            df = spark.read.format("parquet").load(STAGE1_Configurator_Path+"/tblCompanyName")
            df = df.filter(col('DBName')==DBName).filter(col('NewCompanyName') == EntityName)
            COA = COA.select("No_","Name","AccountType","Income_Balance","Totaling")
            COA=COA.filter(COA['No_']!='SERVER')
            Acc_Sche = Acc_Sche.select("ScheduleName","LineNo_","RowNo_","Description","Totaling",
                                    "TotalingType","ShowOppositeSign","RowType","AmountType")
            Acc_Sche = Acc_Sche.filter(Acc_Sche["ScheduleName"]=="P&L")
            Acc_Sche = Acc_Sche.withColumn("Totaling",when(Acc_Sche["Description"]=='RE Salary', lit(9999999))\
                                        .otherwise(Acc_Sche["Totaling"]))
            Acc_Sche = Acc_Sche.withColumn("Totaling",when(Acc_Sche["Description"]=='Corporate Expenses', lit(8888888))\
                                        .otherwise(Acc_Sche["Totaling"]))
            Acc_Sche = Acc_Sche.withColumn("Totaling",when(Acc_Sche["Description"]=='SUB sbu sharing', lit(7777777))\
                                        .otherwise(Acc_Sche["Totaling"]))
            Acc_Sche = Acc_Sche.withColumn("Totaling",when(Acc_Sche["Description"]=='Shared Depreciation', lit(6666666))\
                                        .otherwise(Acc_Sche["Totaling"]))
            delete = udf(lambda s: s.replace("*100", ""), StringType())
            Acc_Sche = Acc_Sche.withColumn("test", delete("Totaling"))
            Acc_Sche = Acc_Sche.withColumn("Percent" , when(Acc_Sche["Totaling"]!=Acc_Sche["test"],"%")\
                                                                    .otherwise("V")).drop("Totaling")\
                                                                    .withColumnRenamed("test","Totaling")
            Acc_Sche = Acc_Sche.withColumn('LineType',when(length(Acc_Sche.Totaling)==0, lit('H'))\
                                                    .when(Acc_Sche.TotalingType==2, lit('F')).otherwise(lit('')))
            Acc_Sche_null=Acc_Sche.filter(Acc_Sche.Totaling=='')
            Acc_Sche_not_null=Acc_Sche.filter(Acc_Sche.Totaling!='')
            row_COA = [int(i.No_) for i in COA.select('No_').collect()]
            df=Acc_Sche_not_null.withColumn("Totaling", explode(split("Totaling", "[|]")))
            df = Kockpit.divide(df, "/", "Totaling", "Totaling",spark) 
            df = df.sort('LineNo_')
            df_minus=Kockpit.only_minus(df,"-","Totaling","Totaling",spark)  
            dfm_plus=Kockpit.plus(df_minus,"[+]","Totaling","Totaling")   
            dfm_plus=dfm_plus.withColumn('Totaling', regexp_replace('Totaling', '\\(|\\)', ''))
            dfm_plus.sort("LineNo_")
            dfn_dot=dfm_plus.filter(~dfm_plus.Totaling.like("%..%"))
            df_dot = dfm_plus.filter(dfm_plus.Totaling.like("%..%"))
            df_dot = Kockpit.addColumnIndex(df_dot,spark)
            d = [i.Totaling for i in df_dot.select('Totaling').collect()]
            dot=[]
            for i in range(0,len(d)):
                s=d[i].split("..")
                l=[]
                for i in range(int(s[0]),int(s[1])+1):
                    if i in row_COA:
                        l.append(i)
                q = '|'.join(str(e) for e in l)
                dot.append(q)
            kdot=sqlCtx.createDataFrame(dot,StringType())
            kdot = Kockpit.addColumnIndex(kdot,spark)
            kdot = kdot.withColumnRenamed('value','dot_splitted')
            df_dot=df_dot.join(kdot,["id"]).drop('id')
            df_dot = df_dot.withColumn("Totaling", explode(split("dot_splitted","[|]")))
            df_dot=df_dot.drop("dot_splitted")
            df_con=dfn_dot.union(df_dot)
            for i in range(0,10):
                seperated=df_con.select(["RowNo_","Totaling","Neg_Flag","RowType"])
                seperated=seperated.withColumnRenamed("Totaling","join"+str(i))\
                            .withColumnRenamed("RowNo_","join"+str(i)+"_RowNo_")\
                            .withColumnRenamed("Neg_Flag","join"+str(i)+"_Neg_Flag")\
                            .withColumnRenamed("RowType","join"+str(i)+"_RowType")
        
                con = df_con["Totaling"] == seperated["join"+str(i)+"_RowNo_"]
                df_con = df_con.join(seperated,con,'left')
                df_con = df_con.withColumn("join"+str(i),coalesce(df_con["join"+str(i)],df_con["Totaling"]))
                df_con = df_con.na.fill({"join"+str(i)+"_Neg_Flag":1})
                df_con = df_con.withColumn("Flag_"+str(i+1),(df_con["join"+str(i)+"_Neg_Flag"]*df_con["Neg_Flag"]).cast("int"))
                df_con = df_con.drop("Neg_Flag").drop("join"+str(i)+"_Neg_Flag")
                df_con = df_con.withColumn("RowType_"+str(i+1),(df_con["join"+str(i)+"_RowType"]+df_con["RowType"]).cast("int"))
                df_con = df_con.withColumn("RowType_"+str(i+1),coalesce(df_con["RowType_"+str(i+1)],df_con["RowType"]))
                df_con = df_con.drop("RowType").drop("join"+str(i)+"_RowType")
                df_con = df_con.drop('Totaling')
                df_con = df_con.withColumnRenamed("Flag_"+str(i+1),"Neg_Flag")\
                            .withColumnRenamed("RowType_"+str(i+1),"RowType")\
                            .withColumnRenamed("join"+str(i),"Totaling")
                df_con = df_con.sort('LineNo_')
                df_con = df_con.na.fill({"join"+str(i)+"_RowNo_":0})
                df_con = df_con.withColumn("join"+str(i)+"_RowNo_",(df_con["join"+str(i)+"_RowNo_"]).cast('int'))
                sum_of_col = sum(df_con.select("join"+str(i)+"_RowNo_").rdd.flatMap(lambda x: x).collect())
                if sum_of_col == 0:
                    df_con = df_con.withColumnRenamed('Totaling','MISKEY')
                    break
        
            MIS = df_con.withColumn('DBName',lit(DBName))\
                        .withColumn('EntityName',lit(EntityName))
            MIS = MIS.withColumn('Link_GL_Key',concat(MIS['DBName'],lit('|'),MIS['EntityName'],lit('|'),MIS['MISKEY']))
            MIS=CONCATENATE(MIS,Acc_Sche_null,spark)
            MIS.cache()
            print(MIS.count())
            MIS.coalesce(1).write.format("parquet").mode("overwrite").option("overwriteSchema", "true").save(STAGE2_PATH+"/"+"Finance/MISPNL")
            logger.endExecution()
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"
        
            log_dict = logger.getSuccessLoggedRecord("Finance.MISPNL", DBName, EntityName, MIS.count(), len(MIS.columns), IDEorBatch)
            log_df = spark.createDataFrame(log_dict, logger.getSchema())
            log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)
            #
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
            os.system("spark-submit "+Kockpit_Path+"/Email.py 1 MISPNL '"+CompanyName+"' "+DBEntity+" "+str(exc_traceback.tb_lineno)+" ")
    
            log_dict = logger.getErrorLoggedRecord('Finance.MISPNL', DBName, EntityName, str(ex), str(exc_traceback.tb_lineno), IDEorBatch)
            log_df = spark.createDataFrame(log_dict, logger.getSchema())
            log_df.write.jdbc(url=PostgresDbInfo.PostgresUrl, table="logs.logs", mode='append', properties=PostgresDbInfo.props)
print('finance_MISPNL completed: ' + str((dt.datetime.now()-st).total_seconds()))     


