import datetime
import datetime as dt
import multiprocessing
import threading, queue
from threading import Lock
from multiprocessing.pool import ThreadPool
from pyspark.sql import functions as F
import pandas as pd
import os,sys,subprocess
import datetime as dt
from os.path import dirname, join, abspath
Kockpit_Path =abspath(join(join(dirname(__file__),'..','..')))
DB_path =abspath(join(join(dirname(__file__),'..')))
sys.path.insert(0,'../../')
sys.path.insert(0, DB_path)
Conf_path =abspath(join(join(dirname(__file__),'..','..')))
sys.path.insert(0, Conf_path)
root_directory =abspath(join(join(dirname(__file__),'..','..')))
root_directory=root_directory+"/"
DBList=[]
st = dt.datetime.now()
for folders in os.listdir(root_directory):
    if os.path.isdir(os.path.join(root_directory,folders)):
        if 'DB' in folders:
            if 'DB0' in folders:
                pass
            else:
                DBList.insert(0,folders )
Connection =abspath(join(join(dirname(__file__), '..'),'..',DBList[0]))
sys.path.insert(0, Connection)
from Stage.Script.Masters.Calendar import masters_calendar
from Stage.Script.Masters.BRANCH_Dimension import masters_BRANCH_Dimension
from Stage.Script.Masters.BU_Dimension import masters_BU_Dimension
from Stage.Script.Masters.CUSTOMER_Dimension import masters_CUSTOMER_Dimension
from Stage.Script.Masters.OTBRANCH_Dimension import masters_OTBRANCH_Dimension
from Stage.Script.Masters.PRODUCT_Dimension import masters_PRODUCT_Dimension
from Stage.Script.Masters.PROJECT_Dimension import masters_PROJECT_Dimension
from Stage.Script.Masters.SBU_Dimension import masters_SBU_Dimension
from Stage.Script.Masters.SUBBU_Dimension import masters_SUBBU_Dimension
from Stage.Script.Masters.TARGETPROD_Dimension import masters_TARGETPROD_Dimension
from Stage.Script.Masters.ChartofAccounts import masters_coa
from Stage.Script.Masters.Customer import masters_customers
from Stage.Script.Masters.Salesperson import masters_Salesperson
from Stage.Script.Masters.Vendor import masters_Vendor
from Stage.Script.Masters.ChartofAccounts import masters_coa
from Stage.Script.Masters.Location import masters_Location
from Stage.Script.Masters.Item import masters_Item
from Stage.Script.Masters.Employee import masters_Employee
from PowerBI_Masters_Refresh import PowerBI_Masters_Refresh

from Stage.Script.Sales.Receivables import sales_Receivables
from Stage.Script.Sales.Receivables_Snapshot import sales_Receivables_Snapshot
from Stage.Script.Sales.ManualCOGS import sales_ManualCOGS
from Stage.Script.Sales.SalesGLEntry import sales_SalesGLEntry
from Stage.Script.Sales.Sales import sales_Sales
from Stage.Script.Sales.SalesOrder import sales_SalesOrder
from Stage.Script.Sales.SalesTarget import sales_SalesTarget
from PowerBI_Sales_Refresh import PowerBI_Sales_Refresh

from Stage.Script.Purchase.Payables_Snapshot import purchase_Payables_Snapshot
from Stage.Script.Purchase.PurchaseArchive import purchase_PurchaseArchive
from Stage.Script.Purchase.PurchaseArchive2 import purchase_PurchaseArchive2
from Stage.Script.Purchase.PurchaseCreditMemo import purchase_PurchaseCreditMemo
from Stage.Script.Purchase.PurchaseCRMemo import purchase_PurchaseCRMemo  
from Stage.Script.Purchase.PurchaseInvoice import purchase_PurchaseInvoice
from Stage.Script.Purchase.PurchaseOrder import purchase_PurchaseOrder
from Stage.Script.Purchase.PurchasePayment import purchase_PurchasePayment
from PowerBI_Purchase_Refresh import PowerBI_Purchase_Refresh

from Stage.Script.Finance.BalanceSheet import finance_BalanceSheet
from Stage.Script.Finance.Budget import finance_Budget
from Stage.Script.Finance.CashFlow import finance_CashFlow
from Stage.Script.Finance.Collection import finance_Collection
from Stage.Script.Finance.MISPNL import finance_MISPNL
from Stage.Script.Finance.ProfitLoss import finance_ProfitLoss
from PowerBI_Finance_Refresh import PowerBI_Finance_Refresh


from Stage.Script.Inventory.StockAgeing import inventory_StockAgeing
from PowerBI_Inventory_Refresh import PowerBI_Inventory_Refresh

print('Stage_DB0  Transformation: ', datetime.datetime.now())
moduleName = sys.argv[1] if len(sys.argv) > 1 else ''

try:   
    print('DB0 Transformation start: ', datetime.datetime.now())
    
    #------------------------------------ MASTERS -------------------------------------
            
    if moduleName == '' or moduleName == 'Masters':
        masters_calendar()
        masters_coa()
        masters_BRANCH_Dimension()
        masters_BU_Dimension()
        masters_CUSTOMER_Dimension()
        masters_OTBRANCH_Dimension()
        masters_PRODUCT_Dimension()
        masters_PROJECT_Dimension()
        masters_PROJECT_Dimension()
        masters_SBU_Dimension()
        masters_SUBBU_Dimension()
        masters_TARGETPROD_Dimension()
        masters_customers()
        masters_Salesperson()
        masters_Vendor()
        masters_Location()
        masters_Item()
        masters_Employee()
        PowerBI_Masters_Refresh()
    #------------------------------------ Sales ----------------------------------------
    if moduleName == '' or moduleName == 'Sales':
        sales_Receivables()
        sales_Receivables_Snapshot()
        sales_Sales()
        sales_ManualCOGS()
        sales_SalesGLEntry()
        sales_SalesOrder()
        sales_SalesTarget()
        PowerBI_Sales_Refresh()
    #------------------------------------ Purchase --------------------------------------
    if moduleName == '' or moduleName == 'Purchase':
        purchase_Payables_Snapshot()
        purchase_PurchaseArchive()
        purchase_PurchaseArchive2()
        purchase_PurchaseCreditMemo()
        purchase_PurchaseCRMemo()
        purchase_PurchaseInvoice()
        purchase_PurchaseOrder()
        purchase_PurchasePayment()
        PowerBI_Purchase_Refresh()
    #------------------------------------ Finance --------------------------------------
    if moduleName == '' or moduleName == 'Finance':
        finance_BalanceSheet()
        finance_Budget()
        finance_CashFlow()
        finance_Collection()
        finance_MISPNL()
        finance_ProfitLoss()
        PowerBI_Finance_Refresh()
    #------------------------------------ Inventory --------------------------------------
    if moduleName == '' or moduleName == 'Inventory':
        inventory_StockAgeing()
        PowerBI_Inventory_Refresh()
        
    print('DB0 Transformation Ended: ' + str((dt.datetime.now()-st).total_seconds()))
except Exception as ex:
    print(ex)
    exc_type,exc_value,exc_traceback=sys.exc_info()

