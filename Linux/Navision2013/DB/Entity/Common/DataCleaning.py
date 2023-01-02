from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession,Row
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
from pyspark.sql import functions as F
import pandas as pd
import os,sys,subprocess
from os.path import dirname, join, abspath
Kockpit_Path =abspath(join(join(dirname(__file__),'..','..','..')))
DB_path =abspath(join(join(dirname(__file__),'..','..')))
Entity_Path=abspath(join(join(dirname(__file__),'..')))
st = dt.datetime.now()
sys.path.insert(0,'../../')
sys.path.insert(0, DB_path)
sys.path.insert(0, Entity_Path)
Conf_path =abspath(join(join(dirname(__file__),'..','..')))
sys.path.insert(0, Conf_path)
from Configuration import  AppConfig as ac
from Configuration.Constant import *
from Configuration.udf import *
from Configuration import udf 


from Stage1.Script.DataIngestion import vacuum

from Stage2.Script.Masters.Calendar import vacuum_calendar
from Stage2.Script.Masters.ChartofAccounts import vacuum_coa
from Stage2.Script.Masters.Customer import vacuum_customers
from Stage2.Script.Masters.Dimensions import vacuum_dimensions
from Stage2.Script.Masters.DSE import vacuum_dse
from Stage2.Script.Masters.Salesperson import vacuum_salesPerson
from Stage2.Script.Masters.Vendor import vacuum_vendor
from Stage2.Script.Masters.Location import vacuum_Location
from Stage2.Script.Masters.Item import vacuum_Item
from Stage2.Script.Masters.Employee import vacuum_employee

from Stage2.Script.Sales.Receivables import vacuum_Receivables
from Stage2.Script.Sales.Receivables_Snapshot import vacuum_Receivables_Snapshot
from Stage2.Script.Sales.Sales import vacuum_SalesGLEntry
from Stage2.Script.Sales.Sales import vacuum_ManualCOGS
from Stage2.Script.Sales.Sales import vacuum_Sales
from Stage2.Script.Sales.SalesOrder import vacuum_SalesOrder
from Stage2.Script.Sales.SalesTarget import vacuum_SalesTarget

from Stage2.Script.Purchase.Payables_Snapshot import vacuum_Payable_Snapshot
from Stage2.Script.Purchase.PurchaseArchive import vacuum_PurchaseArchive
from Stage2.Script.Purchase.PurchaseArchive2 import vacuum_PurchaseArchive2
from Stage2.Script.Purchase.PurchaseCreditMemo import vacuum_PurchaseCreditMemo
from Stage2.Script.Purchase.PurchaseCRMemo import vacuum_PurchaseCRMemo  
from Stage2.Script.Purchase.PurchaseInvoice import vacuum_PurchaseInvoice
from Stage2.Script.Purchase.PurchaseOrder import vacuum_PurchaseOrder
from Stage2.Script.Purchase.PurchasePayment import vacuum_PurchasePayment

from Stage2.Script.Finance.BalanceSheet import vacuum_BalanceSheet
from Stage2.Script.Finance.Budget import vacuum_Budget
from Stage2.Script.Finance.CashFlow import vacuum_CashFlow
from Stage2.Script.Finance.Collection import vacuum_Collection
from Stage2.Script.Finance.MISPNL import vacuum_MISPNL
from Stage2.Script.Finance.ProfitLoss import vacuum_ProfitLoss

from Stage2.Script.Inventory.StockAgeing import vacuum_StockAgeing


print('Data Cleaning Started ', datetime.datetime.now())
moduleName = sys.argv[1] if len(sys.argv) > 1 else ''

try:   
    print('Data Cleaning start: ', datetime.datetime.now())
    
    #------------------------------------ DataIngestion ----------------------------------------
    if moduleName == '' or moduleName == 'vacuum_ParquetData' or moduleName =='vacuum_Configurator':
        vacuum(moduleName)
        
    #------------------------------------ Masters ----------------------------------------
    if moduleName == '' or moduleName == 'Masters':
        vacuum_calendar()
        vacuum_coa()
        vacuum_dse()
        vacuum_customers()
        vacuum_dimensions()
        vacuum_salesPerson()
        vacuum_vendor()
        vacuum_Location()
        vacuum_Item()
        vacuum_employee()

    #------------------------------------ Sales ----------------------------------------
    if moduleName == '' or moduleName == 'Sales':
        vacuum_Receivables()
        vacuum_Receivables_Snapshot()
        vacuum_SalesGLEntry()
        vacuum_ManualCOGS()
        vacuum_Sales()
        vacuum_SalesOrder()
        vacuum_SalesTarget()
    #------------------------------------ Purchase --------------------------------------
    if moduleName == '' or moduleName == 'Purchase':
        vacuum_Payable_Snapshot()
        vacuum_PurchaseArchive()
        vacuum_PurchaseArchive2()
        vacuum_PurchaseInvoice()
        vacuum_PurchaseCreditMemo()
        vacuum_PurchaseCRMemo()
        vacuum_PurchaseOrder() 
        vacuum_PurchasePayment()         
    #------------------------------------ Finance --------------------------------------
    if moduleName == '' or moduleName == 'Finance':
        vacuum_BalanceSheet()
        vacuum_Budget()
        vacuum_CashFlow()
        vacuum_Collection()   
        vacuum_MISPNL()
        vacuum_ProfitLoss()      
    #------------------------------------ Inventory --------------------------------------
    if moduleName == '' or moduleName == 'Inventory':
        vacuum_StockAgeing()       
    print('Data Cleaning Ended: ' + str((dt.datetime.now()-st).total_seconds()))
except Exception as ex:
    print(ex)
    exc_type,exc_value,exc_traceback=sys.exc_info()

