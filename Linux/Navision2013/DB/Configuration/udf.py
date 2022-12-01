
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession,Row
from pyspark.conf import SparkConf
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, StructField,StructType,IntegerType
import re,os,datetime,time,sys,traceback
from datetime import timedelta, date
from pyspark.sql.functions import col,max as max_,concat,concat_ws,year,when,month,to_date,lit,quarter,expr,sum,count,desc,round,split,last_day,udf,length,explode,split,regexp_replace
from Configuration import AppConfig as ac
from enum import Flag


__all__ = ['JOIN','LJOIN','RJOIN','FULL','RENAME','MONTHSDF','LASTDAY','DRANGE','CONCATENATE',
	  'UNREDUCE','UNALL','BUCKET', 'ToDF', 'ToDFWitoutPrefix', 'ToTrimmedColumnsDF', 'RenameDuplicateColumns','getSparkConfig']

__version__ = '0.1'



def TableRename(tblname):

	table_rename = next (table for table in ac.config["TablesToRename"] if table["Table"] == tblname)
	columns = table_rename["Columns"]
	oldcol = columns.get("oldColumnName")
	newcol = columns.get("newColumnName")
	renamedcol = dict(zip(oldcol, newcol))
	return(renamedcol)

#Cross Join Function
def JOIN(df1,df2):
	"""
	This Function Will Return a DataFrame having Cross Join of Two DataFrames
	Syntax for caling function: df = udf.JOIN(df1,df2)
	Works only for Spark Version > 2.0
	"""
	merged = df1.crossJoin(df2)
	return merged

#Left Join Function
def LJOIN(df1,df2,cond):
	"""
	This Function Will Return a DataFrame having Left Join of Two DataFrames
	Syntax for calling function: df = udf.LJOIN(df1,df2,cond)
	Where cond is Joining Condition e.g.: cond = [df1.col1==df2.col2]
	"""
	merged = df1.join(df2,cond,how='left')
	return merged

#Right Join Function
def RJOIN(df1,df2,cond):
	"""
        This Function Will Return a DataFrame having Right Join of Two DataFrames
        Syntax for calling function: df = udf.RJOIN(df1,df2,cond)
        Where cond is Joining Condition e.g.: cond = [df1.col1==df2.col2]
        """
	merged = df1.join(df2,cond,how='right')
	return merged

#Full Join Function
def FULL(df1,df2,cond):
	"""
        This Function Will Return a DataFrame having Full Join of Two DataFrames
        Syntax for calling function: df = udf.FULL(df1,df2,cond)
        Where cond is Joining Condition e.g.: cond = "column_name"
        """
	merged = df1.join(df2,cond,how='full')
	return merged

#Renaming Columns Functions
def RENAME(df,columns):
	"""
        This Function Will Return a DataFrame having Columns Renamed
        Syntax for calling function: df = udf.RENAME(df, columns)
        Where columns is a dictionary containing old and new names e.g.:
	columns = {'old_name1': 'new_name1', 'old_name2': 'new_name2'}
        """
	if isinstance(columns, dict):
		for old_name, new_name in columns.items():
			df = df.withColumnRenamed(old_name, new_name)
		return df
	else:
		raise ValueError("'columns' should be a dict, like {'old_name_1':'new_name_1', 'old_name_2':'new_name_2'}")

#Function for Months Names
def MONTHSDF(sqlctx):
	"""
	This function Will Return a DataFrame of Months Number With Months Name
	Syntax for calling function: df = udf.MONTHSDF(sqlctx)
	Where sqlctx is the SQLContext which is being created in the code
	"""
	import pandas as pd
	Months_dict = {
                 'MonthNum1':[1,2,3,4,5,6,7,8,9,10,11,12],
                 'Months':['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
                 }
	Months_df = pd.DataFrame(Months_dict)
	Monthsdf = sqlctx.createDataFrame(Months_df)
	return Monthsdf

#Function for Last Day of Month
def LASTDAY(ldate):
	"""
	This function will return Last Day of Month
	Syntax for calling function: varname = udf.LASTDAY(Date)
	Where varname can be any variable name of choice and
	Date is the date for which you want the last day of this month
	"""
	import datetime
	if ldate.month==12:
		return ldate.replace(day-31)
	return ldate.replace(month=ldate.month+1, day=1) - datetime.timedelta(days=1)

#Function for Date Range
def DRANGE(start_date,end_date):
	"""
	This function will return Range of Dates
	Syntax for calling function: udf.DRANGE(Date1,Date2)
	Where Date1 and Date2 are dates for which you want the range
	"""
	from datetime import timedelta
	for n in range(int((end_date - start_date).days)+1):
		yield start_date + timedelta(n)

#Function for Concatenate two DataFrames
def CONCATENATE(DF1,DF2,spark):
	"""
	This Function will Return a Dataframe from Concatenates two dataframes
	Syntax for calling function: df = udf.CONCATENATE(DF1,DF2,spark)
	Where DF1 and DF2 are dataframes to be concatenated and spark is SparkSession
	thas is being created in script
	"""
	import pyspark.sql.functions as F
	DF1_Column = []
	for i in DF1.columns:
		DF1_Column.append({'Column_Name1':i})
	DF1_Column = spark.createDataFrame(DF1_Column)

	DF2_Column = []
	for i in DF2.columns:
		DF2_Column.append({'Column_Name2':i})
	DF2_Column = spark.createDataFrame(DF2_Column)

	superset = DF1_Column.unionAll(DF2_Column).select('Column_Name1')\
			.withColumnRenamed('Column_Name1','superset_columns').distinct()
	join_cond1 = [superset.superset_columns==DF1_Column.Column_Name1]
	join_cond2 = [superset.superset_columns==DF2_Column.Column_Name2]
	superset = LJOIN(superset,DF1_Column,join_cond1)
	superset = LJOIN(superset,DF2_Column,join_cond2)

	superset = superset.select(*(F.col(x).alias('c'+str(ix)) for ix,x in enumerate(superset.columns)))

	superset.createOrReplaceTempView('Table1')
	superset = spark.sql("SELECT '`'AS ct,c0,c1,c2,'' AS c4 FROM Table1")
	superset.createOrReplaceTempView('Table1')
	superset = spark.sql("SELECT concat(ct,c0,ct) AS c0, concat(ct,c1,ct) AS c1,concat(ct,c2,ct) AS c2 FROM Table1").na.fill('\'\'')
	superset.createOrReplaceTempView('Table1')
	superset = spark.sql("SELECT concat(c1,' as ',c0) AS Table1_col,concat(c2,' as ',c0) AS Table2_col FROM Table1").na.fill('\'\'')
	
	superset.cache()
	rcount = superset.count()

	for i in range(0,rcount):
		if i==0:
			vVar1 = superset.select(superset.Table1_col).collect()[i]['Table1_col']
			vVar2 = superset.select(superset.Table2_col).collect()[i]['Table2_col']
		else:
			vVar1 = vVar1+","+superset.select(superset.Table1_col).collect()[i]['Table1_col']
			vVar2 = vVar2+","+superset.select(superset.Table2_col).collect()[i]['Table2_col']

	DF1.createOrReplaceTempView('Table_DF1')
	DF2.createOrReplaceTempView('Table_DF2')
	DF1 = spark.sql("SELECT "+vVar1+" FROM Table_DF1")
	DF2 = spark.sql("SELECT "+vVar2+" FROM Table_DF2")
	DF1 = DF1.unionAll(DF2)
	return DF1

#Function for Doing Union of any number of Dataframes With Unequal Columns
def UNREDUCE(dfs):
	"""
	This Function will Union All the Dataframes
	"""
	import functools
	return functools.reduce(lambda df1,df2:df1.union(df2.select(df1.columns)),dfs)

def UNALL(*argv):
	"""
	This Function will be use for N dataframes and N Columns
	Syntax for Calling Function: df = udf.UNALL(df1,df2)
	"""
	import pyspark.sql.functions as F
	l=[]
	arg=list(argv)
	for k in range(0,len(argv)):
		l=l+arg[k].columns
	for k in range(0,len(arg)):
		l1=[]
		for i in l:
			if i not in arg[k].columns:
				l1.append(i)
	for i in l1:
		arg[k] = arg[k].withColumn(i, F.lit(0))
	last=UNREDUCE(arg)
	return last

#Function for Interval Match For Bucketing
def BUCKET(nodlist,buckets,sqlctx):
	"""
	This Function will Return a DataFrame Having interval match for respective list
	Syntax for calling function: df = udf.BUCKET(nodlist,buckets,sqlctx)
	Where nodlist is the respective data list to be mapped e.g. [30,121,141,400]
	i.e. flatten column (hint use flatMap() to flatten) number of days column to be mapped.
	buckets is the dataframe for your respective buckets, it must have following columns:
	1.LowerLimit 2.UpperLimit 3.BucketName 4.BucketSort
	and sqlctx is the SQLContext created in the script.
	"""
	NoOfRows = len(nodlist)
	NoOfBuck = len(buckets)
	data = []
	for i in range(0,NoOfRows):
		n= nodlist[i]
		for j in range(0,NoOfBuck):
			if int(buckets[j].LowerLimit) <= n <= int(buckets[j].UpperLimit):
				data.append({'Interval':buckets[j].BucketName,'Bucket_Sort':buckets[j].BucketSort,'NOD':n})
				break
	TempBuckets = sqlctx.createDataFrame(data).distinct()
	return TempBuckets
	
def ToDF(sqlCtx, hdfspath, entity):
	df = sqlCtx.sparkSession.read.format("delta").load(hdfspath + "/" + entity["Table"])
	prefix = "".join(list(map(lambda x: x[0], entity["Table"].split(" ")))) + "$"
	if 'Columns' in entity:
		entityColumns = [col.replace(' ', '').replace('(', '').replace(')', '') for col in entity["Columns"]]
	else:
		entityColumns = [col.replace(' ', '').replace('(', '').replace(')', '') for col in df.columns]
	df = df.select([F.col(col).alias(prefix+col) for col in df.columns if col in entityColumns])
	return df, prefix
	
def ToDFWitoutPrefix(sqlCtx, hdfspath, entity, onlySelectedColumns):
	df = sqlCtx.sparkSession.read.format("delta").load(hdfspath + "/" + entity["Table"])
	if onlySelectedColumns == True:
		if 'Columns' in entity:
			entityColumns = [col.replace(' ', '').replace('(', '').replace(')', '') for col in entity["Columns"]]
		else:
			entityColumns = [col.replace(' ', '').replace('(', '').replace(')', '') for col in df.columns]
	else:
		entityColumns = [col.replace(' ', '').replace('(', '').replace(')', '') for col in df.columns]
	df = df.select([F.col(col).alias(col) for col in df.columns if col in entityColumns])
	return df

def ToTrimmedColumnsDF(df):
	df = df.select([F.col(col).alias(col.replace(' ', '').replace('(', '').replace(')', '')) for col in df.columns])
	return df

def RenameDuplicateColumns(dataframe):
	columns = dataframe.columns
	column_indices = [(columns[idx], idx) for idx in range(len(columns))]
	dict = {}
	newNames = []
	for cl in column_indices:
		if cl[0] not in dict:
			dict[cl[0]] = []
		else:
			dict[cl[0]].append(cl[1])

		if len(dict[cl[0]]) == 0:
			newNames.append(cl[0])
		else:
			newName = cl[0] + '_' + str(len(dict[cl[0]]))
			newNames.append(newName)
	dataframe = dataframe.toDF(*newNames)
	return dataframe
	

def add_months(date):
        if date.month < 9 :
            return date.replace(month=3, day=31, year=date.year+1)
        return date.replace(month=3, day=31, year=date.year)
def last_day_of_month(date):
        if date.month == 12:
            return date.replace(day=31)
        return date.replace(month=date.month+1, day=1) - datetime.timedelta(days=1)

def daterange(start_date, end_date):
        for n in range(int ((end_date - start_date).days)):
            yield start_date + timedelta(n)
def addColumnIndex(df,sqlCtx):
        PandasDF = df.toPandas()
        list_data = PandasDF.values.tolist()
        columns = df.columns + ['id']
        indexed = [(*cols, i+1) for i, cols in enumerate(list_data)]
        df_added = sqlCtx.createDataFrame(indexed,columns)
        return df_added
def split_dataframe(df,seperate,target_col,new_col):#seperates the elements of the target columns according to seperator
        df = df.withColumn(new_col, split(target_col, seperate))
        return df
def only_minus(df,seperate,target_col,new_col,sqlCtx):
        df_minus = split_dataframe(df, seperate, target_col, new_col)
        flag=[]
        for j in df_minus.select(new_col).collect():
            s=j.Totaling
            if len(s)==1:
                flag.append(1)
            else:
                flag.append(1)
                for i in range(1,len(s)):
                    flag.append(-1)

        flag_df = sqlCtx.createDataFrame(flag, IntegerType())
        flag_df = addColumnIndex(flag_df,sqlCtx)
        flag_df = flag_df.withColumnRenamed('value','Neg_Flag')
        df_minus = df_minus.withColumn(new_col, explode(df_minus.Totaling))
        df_minus = addColumnIndex(df_minus,sqlCtx)
        df_minus=df_minus.join(flag_df,df_minus.id==flag_df.id)
        df_minus = df_minus.drop('id')
        df_minus = df_minus.sort('LineNo_')
        return df_minus
def divide(df,seperate,target_col,new_col,sqlCtx):
        df_divide = split_dataframe(df, seperate, target_col, new_col)
        divide_flag=[]
        list_col = df_divide.select(new_col).collect()
        for j in list_col:
            j = j.Totaling
            if len(j)==1:
                divide_flag.append("N")
            else:
                divide_flag.append("N")
                for i in range(1,len(j)):
                    divide_flag.append("D")
        flag_df = sqlCtx.createDataFrame(divide_flag, StringType())
        flag_df = addColumnIndex(flag_df,sqlCtx)
        flag_df = flag_df.withColumnRenamed('value','Divisor_Flag')
        df_divide = df_divide.withColumn(new_col, explode(df_divide.Totaling))
        df_divide = addColumnIndex(df_divide,sqlCtx)
        df_divide = df_divide.join(flag_df,df_divide.id==flag_df.id).drop('id')
        return df_divide       
def plus(df,seperate,target_col,new_col):
        df_plus = split_dataframe(df, seperate, target_col, new_col)
        df_plus = df_plus.withColumn(new_col, explode(df_plus[new_col]))

        return df_plus       
       
#Customer Segmentation udfs
def last_day_of_month(date):
        if date.month == 12:
                return date.replace(day=31)
        return date.replace(month=date.month+1, day=1) - datetime.timedelta(days=1)
            
def daterange(start_date,end_date):
        for n in range(int ((end_date - start_date).days)):
                 yield start_date + timedelta(n)

def r_score(x):
    if x <= quintiles['Recency'][.2]:
        return 5
    elif x <= quintiles['Recency'][.4]:
        return 4
    elif x <= quintiles['Recency'][.6]:
        return 3
    elif x <= quintiles['Recency'][.8]:
        return 2
    else:
        return 1
    
def fm_score(x,c):
    if x <= quintiles[c][.2]:
        return 1
    elif x <= quintiles[c][.4]:
        return 2
    elif x <= quintiles[c][.6]:
        return 3
    elif x <= quintiles[c][.8]:
        return 4
    else:
        return 5    
def getSparkConfig(master, appName):
	conf = SparkConf().setMaster(master).setAppName(appName)\
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

	sc = SparkContext.getOrCreate(conf = conf)
	sqlCtx = SQLContext(sc)
	spark = SparkSession.builder.appName(appName).getOrCreate() #sqlCtx.sparkSession #SparkSession.builder.appName("Item").getOrCreate() #
	return sqlCtx, spark   
       

    

       
       
       
       
       
       
       
       
       


