'''
Created on 05 Mar 2021

@author: Prashant
'''
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import csv, io,re,keyring,os,datetime
from datetime import timedelta, date
from pyspark.sql.functions import col,max as max_,concat,concat_ws,year,when,month,to_date,lit,quarter,expr,sum,count,desc,round as col_round,explode, array, split,collect_list
import datetime,time,sys,calendar
from pyspark.sql.types import *
import re
from builtins import str
import pandas as pd
import traceback
import os,sys,subprocess
from os.path import dirname, join, abspath
from distutils.command.check import check
import datetime, time
import datetime as dt
from datetime import datetime

helpersDir = '/home/padmin/KockpitStudio'
sys.path.insert(0, helpersDir)
from ConfigurationFiles.AppConfig import *
from Helpers.Constants import *
from Helpers.udf import *

Datelog = datetime.datetime.now().strftime('%Y-%m-%d')
def finance_ProfitLossCustomised(sqlCtx, spark):
    st = dt.datetime.now()
    logger = Logger()
    #Configurl = "jdbc:postgresql://192.10.15.57/Configurator"

    try:

        GL_Entity = next (table for table in config["TablesToIngest"] if table["Table"] == "G_L Entry")
        GLA_Entity = next (table for table in config["TablesToIngest"] if table["Table"] == "G_L Account")
        Emp_Entity = next (table for table in config["TablesToIngest"] if table["Table"] == "Employee")
        Cust_Entity = next (table for table in config["TablesToIngest"] if table["Table"] == "Customer")

        #------------------------Data Extraction----------------------#

        for entityObj in config["DbEntities"]:
            logger = Logger()
            entityLocation = entityObj["Location"]
            DBName = entityLocation[:3]
            EntityName = entityLocation[-2:]
            hdfspath = STAGE1_PATH + "/" + entityLocation
            postgresUrl = PostgresDbInfo.url.format(entityLocation)
            DBurl = "jdbc:postgresql://192.10.15.134/"+entityLocation
            
            
            GL_Entry_Table = ToDFWitoutPrefix(sqlCtx, hdfspath, GL_Entity,True)
            GL_Account_Table = ToDFWitoutPrefix(sqlCtx, hdfspath, GLA_Entity,True)
            Emp_Table = ToDFWitoutPrefix(sqlCtx, hdfspath, Emp_Entity,True)
            Customer = ToDFWitoutPrefix(sqlCtx, hdfspath, Cust_Entity,True)
               
            Customer =Customer.select('No_','Sector')\
                        .withColumnRenamed('No_','GlobalDimension13Code')

            Query_MIS ="(SELECT * FROM finance.mispnl) AS MIS"
            MIS_Table =  spark.read.format("jdbc").options(url=postgresUrl, dbtable=Query_MIS,\
                            user=PostgresDbInfo.props["user"],password=PostgresDbInfo.props["password"],driver= PostgresDbInfo.props["driver"]).load()
            MIS_Table.cache()
            MIS_Table = MIS_Table.select('Description','MISKEY').groupBy('Description').agg(collect_list("MISKEY").alias('GLAccount'))
            REVENUE_GL_list = MIS_Table.filter(MIS_Table["Description"].like('%REVENUE TOTAL%')).select('GLAccount').collect()[0]['GLAccount']
            EBIDTA_GL_list = MIS_Table.filter(MIS_Table["Description"].like('%EBIDTA%')).filter(~(MIS_Table["Description"].like("%Total%"))).select('GLAccount').collect()[0]['GLAccount']
            DEPRECIATION_GL_list = MIS_Table.filter(MIS_Table["Description"].like('%Depreciation%')).filter(~(MIS_Table["Description"].like("%Shared%"))).select('GLAccount').collect()[0]['GLAccount']
            FINANCE_GL_list = MIS_Table.filter(MIS_Table["Description"].like('%Finance%')).select('GLAccount').collect()[0]['GLAccount']
            OTHERINCOME_GL_list = MIS_Table.filter(MIS_Table["Description"].like('%OTHER INCOME%')).filter(MIS_Table["Description"].like('%TOTAL%')).select('GLAccount').collect()[0]['GLAccount']
            PERSONALEXPENSES_GL_list = MIS_Table.filter(MIS_Table["Description"].like('Personnel Expenses')).select('GLAccount').collect()[0]['GLAccount']
            #print(REVENUE_GL_list)
            #sys.exit()
            ######### FOUND EBIDTA GL LIST #########

            Query_COA = "(SELECT * FROM masters.chartofaccounts) AS COA"
            COA_Table = spark.read.format("jdbc").options(url=postgresUrl, dbtable=Query_COA,\
                                    user=PostgresDbInfo.props["user"],password=PostgresDbInfo.props["password"],driver= PostgresDbInfo.props["driver"]).load()

            data = [{'Level7':'RE Salary','GLAccount':'9999999','DBName':'DB1','EntityName':'E1','Link_GLAccount_Key':'DB1|E1|9999999',
                    'GLRangeCategory':'DE'},{'Level7':'Corporate Expenses','GLAccount':'8888888','DBName':'DB1','EntityName':'E1','Link_GLAccount_Key':'DB1|E1|8888888',
                    'GLRangeCategory':'DE'},{'Level7':'Shared Expenses','GLAccount':'7777777','DBName':'DB1','EntityName':'E1','Link_GLAccount_Key':'DB1|E1|7777777',
                    'GLRangeCategory':'DE'},{'Level7':'Shared Depreciation','GLAccount':'6666666','DBName':'DB1','EntityName':'E1','Link_GLAccount_Key':'DB1|E1|6666666',
                    'GLRangeCategory':'INDE'}]
            df = sqlCtx.createDataFrame(data)
            COA_Table = CONCATENATE(COA_Table,df,spark)
            COA_Table.cache()
            #COA_Table.write.jdbc(url=Postgresurl, table=DBET+".COA_Linux_Customized", mode=owmode, properties=Postgresprop)
            COA_Table.write.jdbc(url=postgresUrl, table="Finance.COA_Linux_Customized", mode='overwrite', properties=PostgresDbInfo.props)
            #sys.exit()
            COA_Table = COA_Table.select("GLAccount","GlRangeCategory").withColumnRenamed("GLAccount","GLAccount1")

            ''' TABLE EXTRACTION FROM CONFIGURATOR "BU SHARED %" and "SUBBU Shared %" '''

            Query_BU_Shared = "(SELECT * FROM "+chr(34)+"BU Shared Percentage"+chr(34)+" ) AS BU_Shared"
            BU_Shared_Percentage_Table = spark.read.format("jdbc").options(url=PostgresDbInfo.Configurl, dbtable=Query_BU_Shared,\
                                                     user=PostgresDbInfo.props["user"],password=PostgresDbInfo.props["password"],driver= PostgresDbInfo.props["driver"]).load()
            BU_Shared_Percentage_Table = BU_Shared_Percentage_Table.withColumn("PostingDate",concat_ws('-',split(BU_Shared_Percentage_Table['PostingDate'],'-').getItem(2),
                                                    split(BU_Shared_Percentage_Table['PostingDate'],'-').getItem(1),
                                                    split(BU_Shared_Percentage_Table['PostingDate'],'-').getItem(0)))
    
            BU_Shared_Percentage_Table = BU_Shared_Percentage_Table.withColumn('PostingDate',to_date(BU_Shared_Percentage_Table['PostingDate'])) 
            BU_Shared_Percentage_Table = BU_Shared_Percentage_Table.withColumn("Link_BU",when(BU_Shared_Percentage_Table["BU_Name"]=='IMS', 1)\
                                                                                            .when(BU_Shared_Percentage_Table["BU_Name"]=='SMAC', 2)\
                                                                                            .when(BU_Shared_Percentage_Table["BU_Name"]=='ES', 3)\
                                                                                            .when(BU_Shared_Percentage_Table["BU_Name"]=='WS', 4)\
                                                                                            .when(BU_Shared_Percentage_Table["BU_Name"]=='BApps', 5)\
                                                                    .otherwise(0))

            BU_Shared_Percentage_Table = BU_Shared_Percentage_Table.withColumn('link_bu_month',concat_ws('|',year(BU_Shared_Percentage_Table["PostingDate"]),
                                                                                                      month(BU_Shared_Percentage_Table["PostingDate"]),
                                                                                                      BU_Shared_Percentage_Table["Link_BU"]))
            BU_Shared_Percentage_Table = BU_Shared_Percentage_Table.withColumn('link_month',concat_ws('|',year(BU_Shared_Percentage_Table["PostingDate"]),
                                                                                                      month(BU_Shared_Percentage_Table["PostingDate"]))).drop("PostingDate")
            BU_Shared_Percentage_Table = BU_Shared_Percentage_Table.withColumn("Percentage",col_round(BU_Shared_Percentage_Table["Percentage"]/100,4))
            BU_Shared_Percentage_Table = BU_Shared_Percentage_Table.select('link_bu_month','Percentage')
            #BU_Shared_Percentage_Table.show()
            #sys.exit()
            ''' BU TABLE EXTRACTION AND MANIPULATION COMPLETED '''
            ''' SUBBU TABLE STARTING'''
            Query_SUBBU_Shared = "(SELECT * FROM "+chr(34)+"Cluster/SUBBU Shared Percentage"+chr(34)+" ) AS SUBBU_Shared"
            SUBBU_Shared_Percentage_Table = spark.read.format("jdbc").options(url=PostgresDbInfo.Configurl, dbtable=Query_SUBBU_Shared,\
                                                     user=PostgresDbInfo.props["user"],password=PostgresDbInfo.props["password"],driver= PostgresDbInfo.props["driver"]).load()
            SUBBU_Shared_Percentage_Table = SUBBU_Shared_Percentage_Table.withColumn("PostingDate",concat_ws('-',split(SUBBU_Shared_Percentage_Table['PostingDate'],'-').getItem(2),
                                                            split(SUBBU_Shared_Percentage_Table['PostingDate'],'-').getItem(1),
                                                            split(SUBBU_Shared_Percentage_Table['PostingDate'],'-').getItem(0)))

            SUBBU_Shared_Percentage_Table = SUBBU_Shared_Percentage_Table.withColumn('PostingDate',to_date(SUBBU_Shared_Percentage_Table['PostingDate']))\
                                                                        .withColumn('SUBBU_Code',SUBBU_Shared_Percentage_Table['SUBBU_Code'].cast('int'))

            SUBBU_Shared_Percentage_Table = SUBBU_Shared_Percentage_Table.withColumn('link_subbu_month',concat_ws('|',year(SUBBU_Shared_Percentage_Table["PostingDate"]),
                                                                                                      month(SUBBU_Shared_Percentage_Table["PostingDate"]),
                                                                                                      SUBBU_Shared_Percentage_Table["SUBBU_Code"]))
            SUBBU_Shared_Percentage_Table = SUBBU_Shared_Percentage_Table.withColumn('link_month',concat_ws('|',year(SUBBU_Shared_Percentage_Table["PostingDate"]),
                                                                                                      month(SUBBU_Shared_Percentage_Table["PostingDate"]))).drop("PostingDate")
            SUBBU_Shared_Percentage_Table = SUBBU_Shared_Percentage_Table.withColumn("Percentage",col_round(SUBBU_Shared_Percentage_Table["Percentage"]/100,4))
            #SUBBU_Shared_Percentage_Table.filter(SUBBU_Shared_Percentage_Table['link_month']=='2020|5').show()
            #sys.exit()
            SUBBU_Shared_Percentage_Table = SUBBU_Shared_Percentage_Table.select('link_subbu_month','Percentage')

            '''INS SHARED Percentage table'''

            Query_INS_Shared = "(SELECT * FROM "+chr(34)+"BU Shared Percentage INS"+chr(34)+" ) AS INS_Shared"
            INS_Shared_Percentage_Table =spark.read.format("jdbc").options(url=PostgresDbInfo.Configurl, dbtable=Query_INS_Shared,\
                                                     user=PostgresDbInfo.props["user"],password=PostgresDbInfo.props["password"],driver= PostgresDbInfo.props["driver"]).load()

            INS_Shared_Percentage_Table = INS_Shared_Percentage_Table.withColumn("Link_BU",when(INS_Shared_Percentage_Table["BU_Name"]=='IMS', 1)\
                                                                                            .when(INS_Shared_Percentage_Table["BU_Name"]=='SMAC', 2)\
                                                                                            .when(INS_Shared_Percentage_Table["BU_Name"]=='ES', 3)\
                                                                                            .when(INS_Shared_Percentage_Table["BU_Name"]=='WS', 4)\
                                                                                            .when(INS_Shared_Percentage_Table["BU_Name"]=='BApps', 5)\
                                                                    .otherwise(0))

            INS_Shared_Percentage_Table = INS_Shared_Percentage_Table.withColumn('link_bu_month',concat_ws('|',year(INS_Shared_Percentage_Table["PostingDate"]),
                                                                                                      month(INS_Shared_Percentage_Table["PostingDate"]),
                                                                                                      INS_Shared_Percentage_Table["Link_BU"]))
            INS_Shared_Percentage_Table = INS_Shared_Percentage_Table.withColumn('link_month',concat_ws('|',year(INS_Shared_Percentage_Table["PostingDate"]),
                                                                                                      month(INS_Shared_Percentage_Table["PostingDate"]))).drop("PostingDate")
            INS_Shared_Percentage_Table = INS_Shared_Percentage_Table.withColumn("Percentage",col_round(INS_Shared_Percentage_Table["Percentage"]/100,4))
            INS_Shared_Percentage_Table = INS_Shared_Percentage_Table.select('link_bu_month','Percentage')

            ''' Region Percentage
                Table Extraction '''
            Query_Region_Shared = "(SELECT * FROM "+chr(34)+"Cluster/SUBBU Shared Region Percentage"+chr(34)+" ) AS Region_Shared"
            Region_Shared_Percentage_Table = spark.read.format("jdbc").options(url=PostgresDbInfo.Configurl, dbtable=Query_Region_Shared,\
                                                     user=PostgresDbInfo.props["user"],password=PostgresDbInfo.props["password"],driver= PostgresDbInfo.props["driver"]).load()


            Region_Shared_Percentage_Table = Region_Shared_Percentage_Table.withColumn("Link_BU",when(Region_Shared_Percentage_Table["BU"]=='IMS', 1)\
                                                                                            .when(Region_Shared_Percentage_Table["BU"]=='SMAC', 2)\
                                                                                            .when(Region_Shared_Percentage_Table["BU"]=='ES', 3)\
                                                                                            .when(Region_Shared_Percentage_Table["BU"]=='WS', 4)\
                                                                                            .when(Region_Shared_Percentage_Table["BU"]=='BApps', 5)\
                                                                    .otherwise(0))

            Region_Shared_Percentage_Table = Region_Shared_Percentage_Table.withColumn('link_otbranch_bu_month',concat_ws('|',year(Region_Shared_Percentage_Table["PostingDate"]),
                                                                                                      month(Region_Shared_Percentage_Table["PostingDate"]),
                                                                                                      Region_Shared_Percentage_Table["Link_BU"],
                                                                                                      Region_Shared_Percentage_Table["OTBRANCH Code"]))
            Region_Shared_Percentage_Table = Region_Shared_Percentage_Table.withColumn('link_month',concat_ws('|',year(Region_Shared_Percentage_Table["PostingDate"]),
                                                                                                      month(Region_Shared_Percentage_Table["PostingDate"]))).drop("PostingDate")
            Region_Shared_Percentage_Table = Region_Shared_Percentage_Table.withColumn("Percentage",col_round(Region_Shared_Percentage_Table["Percentage"]/100,4))
            Region_Shared_Percentage_Table = Region_Shared_Percentage_Table.select('link_otbranch_bu_month','Percentage')


            ''' RE Salary
                Table Extraction '''
            Query_RESalary = "(SELECT * FROM "+chr(34)+"RESalary1920"+chr(34)+" ) AS Resalary"
            RE_SALARY_Table = spark.read.format("jdbc").options(url=PostgresDbInfo.Configurl, dbtable=Query_RESalary,\
                                                     user=PostgresDbInfo.props["user"],password=PostgresDbInfo.props["password"],driver= PostgresDbInfo.props["driver"]).load()


            RE_SALARY = RE_SALARY_Table.withColumn("Link_BU",when((RE_SALARY_Table['Link_SUBBU']>=300) & (RE_SALARY_Table['Link_SUBBU']<=399), lit(1))\
                                                                .when((RE_SALARY_Table['Link_SUBBU']>=400) & (RE_SALARY_Table['Link_SUBBU']<=499), lit(2))\
                                                                .otherwise(lit(5)))
            RE_SALARY = RE_SALARY.withColumn('DBName',lit(DBName))\
                                .withColumn('EntityName',lit(EntityName))

            RE_SALARY = RE_SALARY.withColumn("LinkDate",concat_ws('-',split(RE_SALARY['LinkDate'],'-').getItem(2),
                                                            split(RE_SALARY['LinkDate'],'-').getItem(1),
                                                            split(RE_SALARY['LinkDate'],'-').getItem(0)))

            RE_SALARY = RE_SALARY.withColumn('LinkDate',to_date(RE_SALARY['LinkDate']))

            RE_SALARY = RE_SALARY.withColumn('year',year(RE_SALARY['LinkDate']))\
                                .withColumn('month',month(RE_SALARY['LinkDate']))

            RE_SALARY = RE_SALARY.withColumn('Link_BU_Key',concat_ws('|',RE_SALARY['DBName'],RE_SALARY['EntityName'],RE_SALARY['Link_BU']))\
                                .withColumn('Link_GLAccount_Key',concat_ws('|',RE_SALARY['DBName'],RE_SALARY['EntityName'],RE_SALARY['GLAccount']))\
                                .withColumn('Link_SUBBUKey',concat_ws('|',RE_SALARY['DBName'],RE_SALARY['EntityName'],RE_SALARY['Link_SUBBU']))
            RE_SALARY = RE_SALARY.drop('BU')



            ####################### DB Credentials  ###########################
            Query_Company="(SELECT *\
                        FROM "+chr(34)+"tblCompanyName"+chr(34)+") AS df"
            Company = spark.read.format("jdbc").options(url=PostgresDbInfo.Configurl, dbtable=Query_Company,\
                            user=PostgresDbInfo.props["user"],password=PostgresDbInfo.props["password"],driver= PostgresDbInfo.props["driver"]).load()
            Company = Company.filter(col('DBName')==DBName).filter(col('NewCompanyName') == EntityName)
            #Company = Company.filter(Company['DBName'] == DBName ).filter(Company['EntityName'] == EntityName)
            Company = Company.select("StartDate","EndDate")
            Calendar_StartDate = Company.select('StartDate').rdd.flatMap(lambda x: x).collect()[0]
            Calendar_StartDate = datetime.datetime.strptime(Calendar_StartDate,"%m/%d/%Y").date()
            if datetime.date.today().month>int(MnSt)-1:
                UIStartYr=datetime.date.today().year-int(yr)+1
            else:
                UIStartYr=datetime.date.today().year-int(yr)
            UIStartDate=datetime.date(UIStartYr,int(MnSt),1)
            UIStartDate=max(Calendar_StartDate,UIStartDate)
            UIStartDate = '2019-04-01'
            #print(UIStartDate)
            #-------------------------DATA-----------------------------#
            GL_Account_Table = GL_Account_Table.select("No_","Income_Balance")\
                                            .withColumnRenamed('No_','No').drop('DBName','EntityName')

            GL_Entry_Table = GL_Entry_Table.withColumn('PostingDate',to_date(GL_Entry_Table['PostingDate'])).drop('DBName','EntityName')
            GLEntry = GL_Entry_Table.join(GL_Account_Table,GL_Entry_Table['G_LAccountNo_']==GL_Account_Table['No'],'left')

            GLEntry = GLEntry.filter(GLEntry['Income_Balance']==0)\
                            .filter(GLEntry["SourceCode"]!='CLSINCOME')\
                            .filter(year(GLEntry["PostingDate"])!='1753')\
                            .filter(GLEntry["PostingDate"]>=UIStartDate)


            GLEntry = GLEntry.withColumnRenamed("DimensionSetID","DimSetID")\
                                        .withColumnRenamed("EntryNo_","Entry_No")\
                                        .withColumn("LinkDate",GL_Entry_Table["PostingDate"])\
                                        .withColumn("GL_Posting_Date",GL_Entry_Table["PostingDate"])\
                                        .withColumn("linkdoc", concat(GL_Entry_Table["DocumentNo_"],lit('||'),GL_Entry_Table["G_LAccountNo_"]))\
                                        .withColumn("Cost_Amount",GL_Entry_Table["Amount"])\
                                        .withColumnRenamed("DocumentNo_","Document_No")\
                                        .withColumnRenamed("Description","GL_Description")\
                                        .withColumnRenamed("SourceNo_","Source_No")\
                                        .withColumnRenamed("G_LAccountNo_","GLAccount")\
                                        .withColumn("Transaction_Type",lit("GLEntry"))\
                                        .withColumn("Entry_Flag",lit("GL"))

            Query_COA = "(SELECT * FROM masters.chartofaccounts) AS COA"
            COA = spark.read.format("jdbc").options(url=DBurl, dbtable=Query_COA,\
                                            user=PostgresDbInfo.props["user"],password=PostgresDbInfo.props["password"],driver= PostgresDbInfo.props["driver"]).load()
            COA = COA.select('GLAccount','GLRangeCategory')
            GLEntry = GLEntry.join(COA,'GLAccount','left')
            GLEntry = GLEntry.withColumnRenamed("DimSetID","DimensionSetID")
            #------------------------Joining----------------------------
            '''
            GLEntry = GLEntry.join(COA_Table,GLEntry.GLAccount == COA_Table.GLAccount1,'left').drop("GLAccount1")
            table = sqlctx.read.parquet(hdfspath+"/Data/GLCodeExclusion")
            table = table.withColumn("GLExclusionFlag" , lit(1))
            GLExclusion = table.filter(table['DBName'] == DB ).filter(table['EntityName'] == Etn).filter(table['IsExcluded'] == 1)
            GLExclusion = GLExclusion.select("GLCode","GLExclusionFlag")

            ExclusionCond = [GLEntry.GLAccount == GLExclusion.GLCode]
            GLEntry= GLEntry.join(GLExclusion,ExclusionCond,'left').drop("GLCode")
            GLEntry=GLEntry.filter("GLExclusionFlag IS NULL").drop('GLExclusionFlag')
            '''
            #---------------------------Adding Dimension------------------------------
            DSE = sqlCtx.read.parquet(hdfspath + "/DSE").drop("DBName","EntityName")    
            GLEntry =  GLEntry.join(DSE,"DimensionSetID",'left')  
            
            GLEntry = GLEntry.withColumn("LinkDateKey",concat_ws("|",lit(DBName),lit(EntityName),GLEntry.LinkDate))\
                                .withColumn("Link_GLAccount_Key",concat_ws("|",lit(DBName),lit(EntityName),GLEntry.GLAccount))\
                                .withColumn("DBName",lit(DBName))\
                                .withColumn("EntityName",lit(EntityName))

            GLEntry = GLEntry.withColumn('Amount',col_round('Amount',4))\
                                .withColumn('Cost_Amount',col_round('Cost_Amount',4))\
                                .withColumn('CreditAmount',col_round('CreditAmount',4))\
                                .withColumn('DebitAmount',col_round('DebitAmount',4))
            #GLEntry = GLEntry.filter(GLEntry['Document_No']=='RECU1920J-1023').show()
            #sys.exit()

            GLEntry = GLEntry.withColumn("Link_SBU",when(((GLEntry['GLAccount']>=549000) & (GLEntry['GLAccount']<=570000)) | (GLEntry['GLAccount']==405500), GLEntry['GlobalDimension1Code'])\
                                        .otherwise(GLEntry['Link_SBU']))\
                            .withColumn("Link_BRANCH",when(((GLEntry['GLAccount']>=549000) & (GLEntry['GLAccount']<=570000)) | (GLEntry['GLAccount']==405500), GLEntry['GlobalDimension2Code'])\
                                        .otherwise(GLEntry['Link_BRANCH']))\
                            .withColumn("Link_SUBBU",when(((GLEntry['GLAccount']>=549000) & (GLEntry['GLAccount']<=570000)) | (GLEntry['GLAccount']==405500), GLEntry['GlobalDimension14Code'])\
                                        .otherwise(GLEntry['Link_SUBBU']))\
                            .withColumn("Link_OTBRANCH",when(((GLEntry['GLAccount']>=549000) & (GLEntry['GLAccount']<=570000)) | (GLEntry['GLAccount']==405500), GLEntry['GlobalDimension12Code'])\
                                        .otherwise(GLEntry['Link_OTBRANCH']))\
                            .withColumn("Link_CUSTOMER",when(((GLEntry['GLAccount']>=549000) & (GLEntry['GLAccount']<=570000)) | (GLEntry['GLAccount']==405500), GLEntry['GlobalDimension13Code'])\
                                        .otherwise(GLEntry['Link_CUSTOMER']))\
                            .withColumn("Link_PRODUCT",when(((GLEntry['GLAccount']>=549000) & (GLEntry['GLAccount']<=570000)) | (GLEntry['GLAccount']==405500), GLEntry['GlobalDimension11Code'])\
                                        .otherwise(GLEntry['Link_PRODUCT']))

            GLEntry = GLEntry.withColumn("Link_SBUKey",when(((GLEntry['GLAccount']>=549000) & (GLEntry['GLAccount']<=570000)) | (GLEntry['GLAccount']==405500),concat_ws("|",GLEntry['DBName'],GLEntry['EntityName'],GLEntry['Link_SBU']))\
                                                    .otherwise(GLEntry['Link_SBUKey']))\
                            .withColumn("Link_BRANCHKey",when(((GLEntry['GLAccount']>=549000) & (GLEntry['GLAccount']<=570000)) | (GLEntry['GLAccount']==405500),concat_ws("|",GLEntry['DBName'],GLEntry['EntityName'],GLEntry['Link_BRANCH']))\
                                                    .otherwise(GLEntry['Link_BRANCHKey']))\
                            .withColumn("Link_SUBBUKey",when(((GLEntry['GLAccount']>=549000) & (GLEntry['GLAccount']<=570000)) | (GLEntry['GLAccount']==405500),concat_ws("|",GLEntry['DBName'],GLEntry['EntityName'],GLEntry['Link_SUBBU']))\
                                                    .otherwise(GLEntry['Link_SUBBUKey']))\
                            .withColumn("Link_OTBRANCHKey",when(((GLEntry['GLAccount']>=549000) & (GLEntry['GLAccount']<=570000)) | (GLEntry['GLAccount']==405500),concat_ws("|",GLEntry['DBName'],GLEntry['EntityName'],GLEntry['Link_OTBRANCH']))\
                                                    .otherwise(GLEntry['Link_OTBRANCHKey']))\
                            .withColumn("Link_CUSTOMERKey",when(((GLEntry['GLAccount']>=549000) & (GLEntry['GLAccount']<=570000)) | (GLEntry['GLAccount']==405500),concat_ws("|",GLEntry['DBName'],GLEntry['EntityName'],GLEntry['Link_CUSTOMER']))\
                                                    .otherwise(GLEntry['Link_CUSTOMERKey']))\
                            .withColumn("Link_PRODUCTKey",when(((GLEntry['GLAccount']>=549000) & (GLEntry['GLAccount']<=570000)) | (GLEntry['GLAccount']==405500),concat_ws("|",GLEntry['DBName'],GLEntry['EntityName'],GLEntry['Link_PRODUCT']))\
                                                    .otherwise(GLEntry['Link_PRODUCTKey']))

            #####****************************Standard Profit Loss Completed***************************######

            #********************************Mapping Started****************************************#

            GLEntry = GLEntry.withColumn("Link_BU",when(((GLEntry["Link_SUBBU"]>=300) & (GLEntry["Link_SUBBU"]<=399)),lit(1))\
                                         .otherwise(when(((GLEntry["Link_SUBBU"]>=400) & (GLEntry["Link_SUBBU"]<=499)),lit(2))\
                                        .otherwise(when(((GLEntry["Link_SUBBU"]>=101) & (GLEntry["Link_SUBBU"]<=200)),lit(3))\
                                                   .otherwise(when(((GLEntry["Link_SUBBU"]>=10) & (GLEntry["Link_SUBBU"]<=99)),lit(4))\
                                                              .otherwise(when(((GLEntry["Link_SUBBU"]>=500) & (GLEntry["Link_SUBBU"]<=599)),lit(5))\
                                                                .otherwise(when(((GLEntry["Link_SUBBU"]>=700) & (GLEntry["Link_SUBBU"]<=799)),lit(6))))))))

            GLEntry = GLEntry.withColumn("Link_BU_Key",concat_ws('|',GLEntry["DBName"],GLEntry["EntityName"],GLEntry["Link_BU"]))

            GLEntry = GLEntry.withColumn('year',year(GLEntry['PostingDate']))\
                            .withColumn('month',month(GLEntry['PostingDate']))
            GLEntry = GLEntry.withColumn('link_month',concat(GLEntry['year'],lit('|'),GLEntry['month']))

            GLEntry = GLEntry.filter(GLEntry["PostingDate"]<=Datelog)
            GLEntry.cache()

            months = [i.link_month for i in GLEntry.select("link_month").distinct().collect()]


            ''' New Process Started, 27 July
            Changing full process'''

            GLEntry = GLEntry.withColumn("Link_SUBBU",when((GLEntry["Link_SUBBU"]>=400) & (GLEntry["Link_SUBBU"]<=499)
                                                           , when(GLEntry["Link_SBU"]==1601, lit(415))\
                                                             .when(GLEntry["Link_SBU"]==1602, lit(425))\
                                                             .when(GLEntry["Link_SBU"]==1603, lit(420))\
                                                             .when(GLEntry["Link_SBU"]==1604, lit(445))\
                                                             .when(GLEntry["Link_SBU"]==1605, lit(430))\
                                                             .when(GLEntry["Link_SBU"]==1607, lit(495))\
                                                             .when(GLEntry["Link_SBU"]==1609, lit(486))\
                                                             .when(GLEntry["Link_SBU"]==1611, lit(487))\
                                                             .when(GLEntry["Link_SBU"]==1612, lit(488))\
                                                             .when(GLEntry["Link_SBU"]==1613, lit(489))\
                                                             .when(GLEntry["Link_SBU"]==1610, lit(435))\
                                                             .when(GLEntry["Link_SBU"]==1615, lit(440))\
                                                             .when(GLEntry["Link_SBU"]==1620, lit(490))\
                                                             .when(GLEntry["Link_SBU"]==1625, lit(485))\
                                                             .when(GLEntry["Link_SBU"].isin([4551,4560,4670,4680,4799]), lit(455))\
                                                             .when(GLEntry["Link_SBU"].isin([1630]), lit(405))\
                                                             .when((GLEntry["Link_SBU"]>=4250) & (GLEntry["Link_SBU"]<=4499), lit(465))\
                                                             .when((GLEntry["Link_SBU"]>=8000) & (GLEntry["Link_SBU"]<=8100), lit(475))\
                                                             .otherwise(lit(490)))\
                                                        .when((GLEntry["Link_SUBBU"]>=500) & (GLEntry["Link_SUBBU"]<=599),lit(500))\
                                                        .when((GLEntry["Link_SUBBU"]>=10) & (GLEntry["Link_SUBBU"]<=99)
                                                              , when(GLEntry["Link_TARGETPROD"]==70, lit(20))\
                                                              .when(GLEntry["Link_TARGETPROD"].isin([90,95]), lit(30))\
                                                              .when(GLEntry["Link_TARGETPROD"]==100, lit(25))\
                                                              .when(GLEntry["Link_TARGETPROD"]==140, lit(35))\
                                                              .when(GLEntry["Link_TARGETPROD"]==30, lit(40))\
                                                              .when(GLEntry["Link_TARGETPROD"]==25, lit(45))\
                                                              .when(GLEntry["Link_TARGETPROD"]==40, lit(50))\
                                                              .when(GLEntry["Link_TARGETPROD"].isin([170]), lit(55))\
                                                              .when(GLEntry["Link_TARGETPROD"]==80, lit(60))\
                                                              .when(GLEntry["Link_TARGETPROD"].isin([15,20,35,45,50,55,60,75,98,110,120,130,141,145,146,175]), lit(65))\
                                                              .otherwise(lit(96)))\
                                                        .when((GLEntry["Link_SUBBU"]>=101) & (GLEntry["Link_SUBBU"]<=200)
                                                              , when(GLEntry["Link_TARGETPROD"].isin([15,60,98,110,120,130,141,145]), lit(110))\
                                                              .when(GLEntry["Link_TARGETPROD"].isin([10,35,45]), lit(105))\
                                                              .when(GLEntry["Link_TARGETPROD"].isin([20,135]), lit(115))\
                                                              .when((GLEntry["Link_SBU"]>=4551) & (GLEntry["Link_SBU"]<=4799), lit(130))\
                                                              .otherwise(lit(120)))\
                                                        .when((GLEntry["Link_SUBBU"]>=300) & (GLEntry["Link_SUBBU"]<=399)
                                                            , when(GLEntry["Link_SBU"]==710, GLEntry["Link_SUBBU"])\
                                                            .when(GLEntry["Link_SBU"]==520, lit(315))\
                                                            .when(GLEntry["Link_SBU"]==530, lit(320))\
                                                            .when(GLEntry["Link_SBU"]==545, lit(325))\
                                                            .when(GLEntry["Link_SBU"]==550, lit(330))\
                                                            .when(GLEntry["Link_SBU"]==555, lit(335))\
                                                            .when(GLEntry["Link_SBU"]==560, lit(340))\
                                                            .when(GLEntry["Link_SBU"]==510, lit(345))\
                                                            .when(GLEntry["Link_SBU"]==630, lit(350))\
                                                            .when(GLEntry["Link_SBU"].isin([601,610,615,625,635,640,645,650]), lit(350))\
                                                            .when(GLEntry["Link_SBU"]==565, lit(360))\
                                                            .otherwise(lit(305)))\
                                                        .otherwise(GLEntry["Link_SUBBU"]))
            GLEntry = GLEntry.withColumn('Link_SUBBU',when(GLEntry['Link_SUBBU']==310, lit(305)).otherwise(GLEntry["Link_SUBBU"])) #EXTRA ADDED ON 12 APR 2020

            GLEntry = GLEntry.join(Customer,'GlobalDimension13Code','left')
            GLEntry = GLEntry.withColumn("Link_SUBBU",when((GLEntry["Link_SUBBU"]>=101) & (GLEntry["Link_SUBBU"]<=199),
                                                           when(GLEntry["Sector"]==2, lit(140)).otherwise(GLEntry['Link_SUBBU']))\
                                                        .otherwise(GLEntry['Link_SUBBU']))
            GLEntry = GLEntry.withColumn("Link_SUBBUKey",concat_ws('|',GLEntry["DBName"],GLEntry["EntityName"],GLEntry["Link_SUBBU"]))

            ''' (RE SALARY) - dataframe consists the data of RE Salary for APR-2019 and May-2019,
            Concatenated with GL Entry, credited to GLAccount of RE-Salary (9999999) &
            Same has been debited to GLAccount in Personal Expenses '''

            GLEntry = CONCATENATE(GLEntry,RE_SALARY,spark)
            GLEntry = GLEntry.withColumn("LinkDate",GLEntry["LinkDate"].cast('date'))
            GLEntry = GLEntry.withColumn("LinkDateKey",concat_ws('|',GLEntry["DBName"],GLEntry["EntityName"],GLEntry["LinkDate"]))

            ''' After that RE SALARY calculating directly from ERP for rest of the months'''

            ''' Collecting RE list from Emp Table '''

            Excluding_link_months = ['2019|4','2019|5','2019|6','2019|7','2019|8','2019|9','2019|10']

            ''' RE Salary part Completed '''

            ''' (6666666) DEPRECIATION & (8888888) SHARED-EXP SPLITTING STARTED for BU's '''
            SharedExp_Depreciation = GLEntry.filter(((GLEntry["Link_SUBBU"]>=700) & (GLEntry["Link_SUBBU"]<=799)) & (GLEntry["GLAccount"].isin(DEPRECIATION_GL_list+EBIDTA_GL_list)))
            #SharedExp_Depreciation = GLEntry.filter(((GLEntry["Link_SUBBU"]>=700) & (GLEntry["Link_SUBBU"]<=799)) & (GLEntry["GLAccount"].isin(EBIDTA_GL_list)))


            SharedExp_Depreciation = SharedExp_Depreciation.withColumn("GLAccount",when((SharedExp_Depreciation["GLAccount"].isin(DEPRECIATION_GL_list))
                                                          , lit(6666666))\
                                                    .otherwise(SharedExp_Depreciation["GLAccount"]))

            SharedExp_Depreciation = SharedExp_Depreciation.withColumn("GLAccount",when((SharedExp_Depreciation["GLAccount"].isin(EBIDTA_GL_list))
                                                          , lit(8888888))\
                                                    .otherwise(SharedExp_Depreciation["GLAccount"]))

            #SharedExp_Depreciation.filter((SharedExp_Depreciation['LinkDate']>='2020-04-01') & (SharedExp_Depreciation['LinkDate']<='2020-04-30')).groupBy('GLAccount').agg({'Amount':'sum'}).sort('GLAccount').show(100,False)
            #sys.exit()

            ''' Adding a debit value of the same amount in GL Entry '''
            minus_cost = SharedExp_Depreciation.withColumn("Amount",SharedExp_Depreciation["Amount"]*(-1))

            SharedExp_Depreciation = SharedExp_Depreciation.withColumn("Link_BU",lit('1-2-3-4-5'))
            SharedExp_Depreciation = SharedExp_Depreciation.withColumn("Link_BU",explode(split(SharedExp_Depreciation["Link_BU"], '-')))
            SharedExp_Depreciation = SharedExp_Depreciation.withColumn('link_bu_month',concat_ws('|',SharedExp_Depreciation["link_month"],SharedExp_Depreciation["Link_BU"]))
            SharedExp_Depreciation = SharedExp_Depreciation.join(BU_Shared_Percentage_Table
                                                         ,SharedExp_Depreciation["link_bu_month"]==BU_Shared_Percentage_Table["link_bu_month"]
                                                         ,'left').drop('link_bu_month')
            SharedExp_Depreciation = SharedExp_Depreciation.withColumn("Amount",col_round(SharedExp_Depreciation["Amount"]*SharedExp_Depreciation["Percentage"],4))


            SharedExp_Depreciation = SharedExp_Depreciation.withColumn("Link_BU_Key",concat_ws('|',SharedExp_Depreciation["DBName"]
                                                                                       ,SharedExp_Depreciation["EntityName"],SharedExp_Depreciation["Link_BU"])).drop("Percentage")

            ''' STARTING SPLITTING in SUBBU's '''
            ''' FILTERED FOR SMAC, WS & ES '''

            SharedExp_Depreciation_for_SMACESWS = SharedExp_Depreciation.filter(SharedExp_Depreciation["Link_BU"].isin([1,2,3,4]))
            #Bapps = SharedExp_Depreciation_for_SMACESWS.filter((SharedExp_Depreciation["Link_SUBBU"]>=500) &(SharedExp_Depreciation["Link_SUBBU"]<=599))
            #SharedExp_Depreciation_for_SMACESWS = SharedExp_Depreciation_for_SMACESWS.filter(~((SharedExp_Depreciation["Link_SUBBU"]>=500) &(SharedExp_Depreciation["Link_SUBBU"]<=599)))
            SharedExp_Depreciation_for_Others = SharedExp_Depreciation.filter(~(SharedExp_Depreciation["Link_BU"].isin([1,2,3,4])))

            #SharedExp_Depreciation_for_Others = SharedExp_Depreciation_for_Others.withColumn("Link_SUBBU",when(SharedExp_Depreciation_for_Others["Link_BU"]==1,lit(310)).otherwise(lit(510)))

            ''' CORPORATE SHARED in IMS '''
            #SharedExp_Depreciation_for_IMS = SharedExp_Depreciation_for_Others.filter(SharedExp_Depreciation_for_Others["Link_BU"]==1)
            #SharedExp_Depreciation_for_Others = SharedExp_Depreciation_for_Others.filter(~(SharedExp_Depreciation_for_Others["Link_BU"]==1))
            SharedExp_Depreciation_for_Others = SharedExp_Depreciation_for_Others.withColumn("Link_SUBBU",when(SharedExp_Depreciation_for_Others["Link_BU"]==5,
                                                                                                            lit(510)).otherwise("Link_SUBBU"))
            SharedExp_Depreciation_for_Others = SharedExp_Depreciation_for_Others.withColumn("Link_SUBBUKey",concat_ws('|',SharedExp_Depreciation_for_Others["DBName"],
                                                                                                                    SharedExp_Depreciation_for_Others["EntityName"],
                                                                                                                    SharedExp_Depreciation_for_Others["Link_SUBBU"]))


            ''' ADDED IMS CORPORATE SHARED '''

            SharedExp_Depreciation_for_SMACESWS = SharedExp_Depreciation_for_SMACESWS.withColumn("Link_SUBBU",when(SharedExp_Depreciation_for_SMACESWS["Link_BU"]==1
                                                                                                ,lit('315-320-325-330-335-340-345-350'))\
                                                                                                            .when(SharedExp_Depreciation_for_SMACESWS["Link_BU"]==2
                                                                                                , lit('435-425-415-445-420-440-430-455-465-475-405-485-486-487-488-489-495'))\
                                                                                                            .when(SharedExp_Depreciation_for_SMACESWS["Link_BU"]==3
                                                                                                ,lit('105-110-115-140-130-160'))\
                                                                                                            .otherwise(lit('20-25-30-35-40-45-50-55-60')))
            SharedExp_Depreciation_for_SMACESWS = SharedExp_Depreciation_for_SMACESWS.withColumn("Link_SUBBU",explode(split(SharedExp_Depreciation_for_SMACESWS["Link_SUBBU"], '-')))
            SharedExp_Depreciation_for_SMACESWS = SharedExp_Depreciation_for_SMACESWS.withColumn('link_subbu_month',concat_ws('|',SharedExp_Depreciation_for_SMACESWS["link_month"]
                                                                                                                              ,SharedExp_Depreciation_for_SMACESWS["Link_SUBBU"]))

            SharedExp_Depreciation_for_SMACESWS = SharedExp_Depreciation_for_SMACESWS.join(SUBBU_Shared_Percentage_Table
                                                         ,SharedExp_Depreciation_for_SMACESWS["link_subbu_month"]==SUBBU_Shared_Percentage_Table["link_subbu_month"]
                                                         ,'left').drop('link_subbu_month')

            #SharedExp_Depreciation_for_SMACESWS.filter(SharedExp_Depreciation_for_SMACESWS['LinkDate']<='2019-04-30').filter(~(SharedExp_Depreciation_for_SMACESWS['Percentage'].isNull())).show()
            #sys.exit()

            SharedExp_Depreciation_for_SMACESWS = SharedExp_Depreciation_for_SMACESWS.withColumn("Amount",col_round(SharedExp_Depreciation_for_SMACESWS["Amount"]*SharedExp_Depreciation_for_SMACESWS["Percentage"],4))
            SharedExp_Depreciation_for_SMACESWS = SharedExp_Depreciation_for_SMACESWS.withColumn("Link_SUBBUKey",concat_ws('|',SharedExp_Depreciation_for_SMACESWS["DBName"]
                                                                                       ,SharedExp_Depreciation_for_SMACESWS["EntityName"],SharedExp_Depreciation_for_SMACESWS["Link_SUBBU"])).drop("Percentage")
            SharedExp_Depreciation = CONCATENATE(SharedExp_Depreciation_for_SMACESWS,SharedExp_Depreciation_for_Others,spark)

            GLEntry = CONCATENATE(GLEntry,SharedExp_Depreciation,spark)
            GLEntry = CONCATENATE(GLEntry,minus_cost,spark)
            GLEntry = GLEntry.withColumn("Link_GLAccount_Key",concat_ws('|',GLEntry["DBName"],GLEntry["EntityName"],GLEntry["GLAccount"]))

            ''' CORPORATE SHARED AND DEPRECIATION DISTRIBUTION COMPLETED TO BU AND SUBBU LEVEL'''

            ''' INS SHARED DISTRIBUTION STARTING '''
            INS_Shared_glentry = GLEntry.filter(GLEntry["Link_SUBBU"]==100)
            GLEntry = GLEntry.filter(~(GLEntry["Link_SUBBU"]==100))
            INS_Shared_glentry = INS_Shared_glentry.withColumn("Link_BU",lit('3-4'))
            INS_Shared_glentry = INS_Shared_glentry.withColumn("Link_BU",explode(split(INS_Shared_glentry["Link_BU"], '-')))

            INS_Shared_glentry = INS_Shared_glentry.withColumn('link_bu_month',concat_ws('|',INS_Shared_glentry["link_month"],INS_Shared_glentry["Link_BU"]))
            INS_Shared_glentry = INS_Shared_glentry.join(INS_Shared_Percentage_Table
                                                         ,INS_Shared_glentry["link_bu_month"]==INS_Shared_Percentage_Table["link_bu_month"]
                                                         ,'left').drop('link_bu_month')
            INS_Shared_glentry = INS_Shared_glentry.withColumn("Amount",col_round(INS_Shared_glentry["Amount"]*INS_Shared_glentry["Percentage"],4))

            INS_Shared_glentry = INS_Shared_glentry.withColumn("Link_BU_Key",concat_ws('|',INS_Shared_glentry["DBName"],INS_Shared_glentry["EntityName"],INS_Shared_glentry["Link_BU"])).drop('Percentage')
            INS_Shared_glentry = INS_Shared_glentry.withColumn("Link_SUBBU",when(INS_Shared_glentry["Link_BU"]==4
                                                                                 , lit(15)).otherwise(lit(120)))
            INS_Shared_glentry = INS_Shared_glentry.withColumn("Link_SUBBUKey",concat_ws('|',INS_Shared_glentry["DBName"],INS_Shared_glentry["EntityName"],INS_Shared_glentry["Link_SUBBU"]))

            GLEntry = CONCATENATE(INS_Shared_glentry,GLEntry,spark)

            ''' INS SHARED ADDED in WS AND ES SHARED '''
            #GLEntry.filter(GLEntry['Document_No']=='JV2021E-0291').show()
            #sys.exit()
            ''' SUBBU SHARED DISTRIBUTION (7777777) STARTING '''

            ''' SMAC WS & ES STARTING '''
            PL_SMAC = GLEntry.filter(GLEntry["Link_BU"].isin([2]))
            PL_ESWS = GLEntry.filter(GLEntry["Link_BU"].isin([3,4]))
            PL_IMS = GLEntry.filter(GLEntry["Link_BU"].isin([1]))

            PL_SMACESWS = GLEntry.filter(GLEntry["Link_BU"].isin([1,2,3,4]))

            PL_Others = GLEntry.filter((GLEntry["Link_BU"].isin([1,2,3,4])==False)|(GLEntry['Link_BU'].isNull()))
            #PL_Others.filter(PL_Others['Document_No']=='JV2021E-0291').show()
            #sys.exit()

            PL_Others = PL_Others.withColumn("Link_SUBBU",when(PL_Others["Link_BU"]==5,lit(510)).otherwise(PL_Others["Link_SUBBU"]))
            PL_Others = PL_Others.withColumn("Link_SUBBUKey",concat_ws('|',PL_Others["DBName"],PL_Others["EntityName"],PL_Others["Link_SUBBU"]))

            SMAC_Expenses = PL_SMAC.filter(PL_SMAC["Link_SUBBU"].isin([490])).filter(PL_SMAC["GLAccount"].isin(EBIDTA_GL_list+DEPRECIATION_GL_list+FINANCE_GL_list))
            Expenses_ESWS = PL_ESWS.filter(PL_ESWS["Link_SUBBU"].isin([96,120])).filter(PL_ESWS["GLAccount"].isin(EBIDTA_GL_list+DEPRECIATION_GL_list+FINANCE_GL_list))
            IMS_Shared = PL_IMS.filter(PL_IMS["Link_SUBBU"].isin([305])).filter(PL_IMS["GLAccount"].isin(EBIDTA_GL_list+DEPRECIATION_GL_list+FINANCE_GL_list))
            Expenses_SMACESWS = CONCATENATE(SMAC_Expenses,Expenses_ESWS,spark)
            Expenses_SMACESWS = CONCATENATE(Expenses_SMACESWS,IMS_Shared,spark)

            Expenses_SMACESWS = Expenses_SMACESWS.withColumn("GLAccount",when(Expenses_SMACESWS["GLAccount"].isin(EBIDTA_GL_list),lit(7777777))\
                                                                        .otherwise(Expenses_SMACESWS["GLAccount"]))
            minus_cost_smacesws_expenses = Expenses_SMACESWS.withColumn("Amount",Expenses_SMACESWS["Amount"]*(-1))

            Expenses_SMACESWS = Expenses_SMACESWS.withColumn("Link_SUBBU",when(Expenses_SMACESWS["Link_BU"]==1
                                                                                                , lit('315-320-325-330-335-340-345-350'))\
                                                                                                            .when(Expenses_SMACESWS["Link_BU"]==2
                                                                                                , lit('435-425-415-445-420-440-430-455-465-475-485-486-487-488-489-495'))\
                                                                                                            .when(Expenses_SMACESWS["Link_BU"]==3
                                                                                                ,lit('105-110-115-140-130-160'))\
                                                                                                            .otherwise(lit('20-25-30-35-40-45-50-55-60')))
            Expenses_SMACESWS = Expenses_SMACESWS.withColumn("Link_SUBBU",explode(split(Expenses_SMACESWS["Link_SUBBU"], '-')))
            Expenses_SMACESWS = Expenses_SMACESWS.withColumn('link_subbu_month',concat_ws('|',Expenses_SMACESWS["link_month"],Expenses_SMACESWS["Link_SUBBU"]))

            Expenses_SMACESWS = Expenses_SMACESWS.join(SUBBU_Shared_Percentage_Table
                                                         ,Expenses_SMACESWS["link_subbu_month"]==SUBBU_Shared_Percentage_Table["link_subbu_month"]
                                                         ,'left').drop('link_subbu_month')
            Expenses_SMACESWS = Expenses_SMACESWS.withColumn("Amount",col_round(Expenses_SMACESWS["Amount"]*Expenses_SMACESWS["Percentage"],4))
            Expenses_SMACESWS = Expenses_SMACESWS.withColumn("Link_SUBBUKey",concat_ws('|',Expenses_SMACESWS["DBName"]
                                                                                       ,Expenses_SMACESWS["EntityName"],Expenses_SMACESWS["Link_SUBBU"])).drop("Percentage")


            PL_SMACESWS = CONCATENATE(Expenses_SMACESWS,PL_SMACESWS,spark)
            GLEntry = CONCATENATE(PL_SMACESWS,PL_Others,spark)
            GLEntry = CONCATENATE(GLEntry,minus_cost_smacesws_expenses,spark)

            GLEntry = GLEntry.withColumn("Link_GLAccount_Key",concat_ws('|',GLEntry['DBName'],GLEntry['EntityName'],GLEntry['GLAccount']))
            #GLEntry.show()
            #sys.exit()
            
            GLEntry = GLEntry.withColumn("GLAccount",when(~(GLEntry["link_month"].isin(Excluding_link_months))\
                                                          , when(GLEntry["RE"]==1,
                                                                 when(GLEntry["GLAccount"].isin(PERSONALEXPENSES_GL_list),lit(9999999))\
                                                                 .otherwise(GLEntry["GLAccount"]))
                                                            .otherwise(GLEntry["GLAccount"]))\
                                                    .otherwise(GLEntry["GLAccount"]))
            GLEntry.cache()                                        
            GLEntry = GLEntry.withColumn("Link_GLAccount_Key",concat_ws('|',GLEntry['DBName'],GLEntry['EntityName'],GLEntry['GLAccount']))                                        
            GLEntry.write.jdbc(url=postgresUrl, table="Finance.ProfitLossCustomised", mode='overwrite', properties=PostgresDbInfo.props)
            Grouped_GLEntry = GLEntry.filter(GLEntry['Document_No']!='')\
                            .groupBy('LinkDate','GLRangeCategory','GLAccount','Link_CUSTOMER','Link_SUBBU','Link_PROJECT','GlobalDimension10Code')\
                            .agg({'Amount':'sum'})\
                            .withColumnRenamed('sum(Amount)','Amount')
            Grouped_GLEntry.write.jdbc(url=Postgresurl, table="Finance.ProfitLoss_Grouped", mode=owmode, properties=Postgresprop)
    
            logger.endExecution()
            
            try:
                IDEorBatch = sys.argv[1]
            except Exception as e :
                IDEorBatch = "IDLE"

            log_dict = logger.getSuccessLoggedRecord("Finance.ProfitLossCustomised", DBName, EntityName, GLEntry.count(), len(GLEntry.columns), IDEorBatch)
            log_df = spark.createDataFrame(log_dict, logger.getSchema())
            log_df.write.jdbc(url=PostgresDbInfo.logsDbUrl, table="logtable", mode='append', properties=PostgresDbInfo.props)
            
            
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
        
        log_dict = logger.getErrorLoggedRecord('Finance.ProfitLossCustomised', DBName, EntityName, str(ex), str(exc_traceback.tb_lineno), IDEorBatch)
        log_df = spark.createDataFrame(log_dict, logger.getSchema())
        log_df.write.jdbc(url=PostgresDbInfo.logsDbUrl, table="logtable", mode='append', properties=PostgresDbInfo.props)  
    print('finance_ProfitLossCustomised completed: ' + str((dt.datetime.now()-st).total_seconds()))

if __name__ == "__main__":
    sqlCtx, spark = getSparkConfig(SPARK_MASTER, "Stage2:ProfitLossCustomised")
    finance_ProfitLossCustomised(sqlCtx, spark)


            


