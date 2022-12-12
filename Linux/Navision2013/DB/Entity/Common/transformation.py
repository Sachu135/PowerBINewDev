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
DB_path =abspath(join(join(dirname(__file__),'..')))
st = dt.datetime.now()
sys.path.insert(0,'../../')
sys.path.insert(0, DB_path)
Conf_path =abspath(join(join(dirname(__file__),'..','..')))
sys.path.insert(0, Conf_path)
from Configuration import  AppConfig as ac
from Configuration.Constant import *
from Configuration.udf import *
from Configuration import udf 

from Stage1.Script.DataIngestion import Reload
from Stage2.Script.Masters.Calendar import masters_calendar
from Stage2.Script.Masters.ChartofAccounts import masters_coa
from Stage2.Script.Masters.Customer import masters_customers
from Stage2.Script.Masters.Dimensions import masters_dimensions
from Stage2.Script.Masters.DSE import masters_dse
from Stage2.Script.Masters.Salesperson import masters_salesPerson
from Stage2.Script.Masters.Vendor import masters_vendor
from Stage2.Script.Masters.ChartofAccounts import masters_coa
from Stage2.Script.Masters.Location import masters_Location
from Stage2.Script.Masters.Item import masters_Item
from Stage2.Script.Masters.Employee import masters_employee

from Stage2.Script.Sales.Receivables import sales_Receivables
from Stage2.Script.Sales.Receivables_Snapshot import sales_Receivables_Snapshot
from Stage2.Script.Sales.Sales import sales_Sales
from Stage2.Script.Sales.SalesOrder import sales_SalesOrder
from Stage2.Script.Sales.SalesTarget import sales_SalesTarget

from Stage2.Script.Purchase.Payables_Snapshot import purchase_Payables_Snapshot
from Stage2.Script.Purchase.PurchaseArchive import purchase_PurchaseArchive
from Stage2.Script.Purchase.PurchaseArchive2 import purchase_PurchaseArchive2
from Stage2.Script.Purchase.PurchaseCreditMemo import purchase_PurchaseCreditMemo
from Stage2.Script.Purchase.PurchaseCRMemo import purchase_PurchaseCRMemo  
from Stage2.Script.Purchase.PurchaseInvoice import purchase_PurchaseInvoice
from Stage2.Script.Purchase.PurchaseOrder import purchase_PurchaseOrder
from Stage2.Script.Purchase.PurchasePayment import purchase_PurchasePayment

from Stage2.Script.Finance.BalanceSheet import finance_BalanceSheet
from Stage2.Script.Finance.Budget import finance_Budget
from Stage2.Script.Finance.CashFlow import finance_CashFlow
from Stage2.Script.Finance.Collection import finance_Collection
from Stage2.Script.Finance.MISPNL import finance_MISPNL
from Stage2.Script.Finance.ProfitLoss import finance_ProfitLoss

from Stage2.Script.Inventory.IAP import inventory_IAP
from Stage2.Script.Inventory.StockAgeing import inventory_StockAgeing


print('Stage 2 Transformation: ', datetime.datetime.now())
moduleName = sys.argv[1] if len(sys.argv) > 1 else ''

try:   
    print('Transformation start: ', datetime.datetime.now())
    
    #------------------------------------ DATAINGESTION -------------------------------------
    if moduleName == '' or moduleName == 'FULL' or moduleName =='CONFIGURATOR':
            Reload(moduleName)
            
    if moduleName == '' or moduleName == 'Masters':
        masters_calendar()
        masters_coa()
        masters_dse()
        masters_customers()
        masters_dimensions()
        masters_salesPerson()
        masters_vendor()
        masters_Location()
        masters_Item()
        masters_employee()
        
    # #------------------------------------ Sales ----------------------------------------
    if moduleName == '' or moduleName == 'Sales':
        sales_Receivables()
        sales_Receivables_Snapshot()
        sales_Sales()
        sales_SalesOrder()
        sales_SalesTarget()
    # #------------------------------------ Purchase --------------------------------------
    if moduleName == '' or moduleName == 'Purchase':
        purchase_Payables_Snapshot()
        purchase_PurchaseArchive()
        purchase_PurchaseArchive2()
        purchase_PurchaseInvoice()
        purchase_PurchaseCreditMemo()
        purchase_PurchaseCRMemo()
        purchase_PurchaseOrder() 
        purchase_PurchasePayment()         
    # #------------------------------------ Finance --------------------------------------
    if moduleName == '' or moduleName == 'Finance':
        finance_BalanceSheet()
        finance_Budget()
        finance_CashFlow()
        finance_Collection()   
        finance_MISPNL()
        finance_ProfitLoss()      
    # #------------------------------------ Inventory --------------------------------------
    if moduleName == '' or moduleName == 'Inventory':
        inventory_IAP()  
        inventory_StockAgeing()       
    print('Transformation Ended: ' + str((dt.datetime.now()-st).total_seconds()))
except Exception as ex:
    print(ex)
    exc_type,exc_value,exc_traceback=sys.exc_info()

