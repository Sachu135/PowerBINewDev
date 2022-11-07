#import library

import psycopg2

import pandas as pd
from datetime import timedelta

from datetime import datetime
import datetime as dt
import psycopg2, sys
from sqlalchemy import create_engine
import pyarrow.parquet as pq
import os

Stage2_Path =os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
print(Stage2_Path)
sys.path.insert(0,'../../../..')

DB_HOST = "172.16.10.186"
DB_NAME="kockpit_new"
DB_USER="postgres"
DB_PASS="sa@123"

Stage2_Path =os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:

    conn=psycopg2.connect(dbname = DB_NAME, user = DB_USER, password = DB_PASS, host = DB_HOST)
    conn.autocommit = True
    cursor = conn.cursor()
    
except Exception as e:
    print(e)
    flag = 1
    

engine = create_engine('postgresql://'+DB_USER+':sa%40123@'+DB_HOST+':5432/'+DB_NAME+'')


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

from psycopg2.extensions import AsIs




Path1 = Stage2_Path+"\ParquetData\Master\Customer"
customer = pd.read_parquet(Path1, engine='fastparquet')
customer = customer[customer.LinkCustomer.notnull()]
customer = customer[['CustomerGroupName','CustomerName','LinkCustomerKey']]
Path2 = Stage2_Path+"\ParquetData\Sales\Sales"
sales = pq.ParquetDataset(Path2)
sales = sales.read().to_pandas()
sales = sales[['DocumentNo_','PostingDate','Amount','Link_CUSTOMERKey','Ship-toCity','Link_PROJECTKey','LinkItemKey']]
sales = sales.rename(columns={'Ship-toCity':'CustomerCity','Amount':'InvoiceAt','Link_CUSTOMERKey':'LinkCustomerKey'})
sales = sales.merge(customer,on='LinkCustomerKey',how='left')
Path3 = Stage2_Path+"\ParquetData\Master\Item"
item = pd.read_parquet(Path3, engine='fastparquet')
item = item[['ItemManufacturer','LinkItemKey','ItemCategoryCode','ItemDescription','BaseUnitofMeasure']]
Path4 = Stage2_Path+"\ParquetData\Master\PROJECT_Dimension"
project =pd.read_parquet(Path4, engine='fastparquet')
project = project[~project.LinkPROJECT.isin(["GS041367\n"])]
project = project[['LinkPROJECTKey','PROJECT_Name']]
project = project.rename(columns={'LinkPROJECTKey':'Link_PROJECTKey'})
Path5 = Stage2_Path+"\ParquetData\Master\RFM_Score"
score =pd.read_parquet(Path5, engine='fastparquet')
sales['LinkDate']= pd.to_datetime(sales['PostingDate'])
sales["LinkDate"] = sales["LinkDate"].dt.date
sales = sales.merge(item , on = 'LinkItemKey', how = 'left')
sales = sales.merge(project , on = 'Link_PROJECTKey', how = 'left')
sales  = sales.groupby(['LinkDate','CustomerGroupName','CustomerName','ItemCategoryCode','ItemDescription','PROJECT_Name']).agg({'InvoiceAt':'sum','DocumentNo_':'count'})
sales.reset_index(inplace=True)
sales['DocumentNo_'] = 1
Sales_Year = sales.copy(deep = True)
Sales_YTD = sales.copy(deep = True)
Unique_key = sales.loc[ : , ['ItemCategoryCode','CustomerGroupName']]
rfm = sales.groupby(['ItemCategoryCode','CustomerGroupName']).agg({'LinkDate':'max','DocumentNo_':'count','InvoiceAt':'sum'})
rfm.reset_index(inplace=True)
rfm['max_date'] = rfm['LinkDate'].max()
rfm['Days'] = (rfm['max_date'] - rfm['LinkDate']).dt.days
rfm.rename(columns = {'Days': 'Recency','DocumentNo_': 'Frequency','InvoiceAt': 'Monetary'}, inplace=True)
quintiles = rfm[['Recency', 'Frequency', 'Monetary']].quantile([.2, .4, .6, .8]).to_dict()
rfm['R'] = rfm['Recency'].apply(lambda x: r_score(x))
rfm['F'] = rfm['Frequency'].apply(lambda x: fm_score(x, 'Frequency'))
rfm['M'] = rfm['Monetary'].apply(lambda x: fm_score(x, 'Monetary'))
rfm['RFM'] = rfm['R'].astype(str)+rfm['F'].astype(str)+rfm['M'].astype(str)
rfm = rfm.drop(['R','F','M'],axis=1)
score['RFM'] = score['RFM'].astype(str)
rfm = rfm.merge(score, on = 'RFM', how ='left')
rfm['LinkDate'] = pd.to_datetime(rfm['LinkDate'])
rfm['max_date'] = pd.to_datetime(rfm['max_date'])
rfm[['ItemCategoryCode', 'CustomerGroupName', 'Segment']] = rfm[['ItemCategoryCode', 'CustomerGroupName', 'Segment']].astype(str)
rfm.to_csv(Stage2_Path+r"\ParquetData\Sales\\"+"Customer_Segmentation.csv")
rfm.to_sql('customer_segmentation', engine,if_exists='replace', schema='sales')

rfm_wi = sales.groupby(['CustomerGroupName']).agg({'LinkDate':'max','DocumentNo_':'count','InvoiceAt':'sum'})
rfm_wi.reset_index(inplace=True)
rfm_wi['max_date'] = rfm_wi['LinkDate'].max()
rfm_wi['Days'] = (rfm_wi['max_date'] - rfm_wi['LinkDate']).dt.days
rfm_wi.rename(columns = {'Days': 'Recency','DocumentNo_': 'Frequency','InvoiceAt': 'Monetary'}, inplace=True)
quintiles = rfm_wi[['Recency', 'Frequency', 'Monetary']].quantile([.2, .4, .6, .8]).to_dict()

rfm_wi['R'] = rfm_wi['Recency'].apply(lambda x: r_score(x))
rfm_wi['F'] = rfm_wi['Frequency'].apply(lambda x: fm_score(x, 'Frequency'))
rfm_wi['M'] = rfm_wi['Monetary'].apply(lambda x: fm_score(x, 'Monetary'))
rfm_wi['rfm_wi'] = rfm_wi['R'].astype(str)+rfm_wi['F'].astype(str)+rfm_wi['M'].astype(str)
rfm_wi = rfm_wi.drop(['R','F','M'],axis=1)

score['rfm_wi'] = score['RFM'].astype(str)
rfm_wi = rfm_wi.merge(score, on = 'rfm_wi', how ='left')
Sales_Year['LinkDate'] = pd.to_datetime(Sales_Year['LinkDate'], format='%Y-%m-%d')
Sales_Year['Year'] = Sales_Year['LinkDate'].dt.year
for i in range(Sales_Year['Year'].min(),Sales_Year['Year'].max()+1):
    S_Temp = Sales_Year[(Sales_Year['Year'] == i)]
    rfm = S_Temp.groupby(['ItemCategoryCode','CustomerGroupName']).agg({'LinkDate':'max','DocumentNo_':'count','InvoiceAt':'sum'})
    rfm.reset_index(inplace=True)
    rfm['max_date'] = rfm['LinkDate'].max()
    rfm['Days'] = (rfm['max_date'] - rfm['LinkDate']).dt.days
    rfm.rename(columns = {'Days': 'Recency','DocumentNo_': 'Frequency','InvoiceAt': 'Monetary'}, inplace=True)
    quintiles = rfm[['Recency', 'Frequency', 'Monetary']].quantile([.2, .4, .6, .8]).to_dict()
    
    rfm['R'] = rfm['Recency'].apply(lambda x: r_score(x))
    rfm['F'] = rfm['Frequency'].apply(lambda x: fm_score(x, 'Frequency'))
    rfm['M'] = rfm['Monetary'].apply(lambda x: fm_score(x, 'Monetary'))
    rfm['RFM'] = rfm['R'].astype(str)+rfm['F'].astype(str)+rfm['M'].astype(str)
    rfm = rfm.drop(['R','F','M'],axis=1)
    
    score['RFM'] = score['RFM'].astype(str)
    rfm = rfm.merge(score, on = 'RFM', how ='left')
    if(i == Sales_Year['Year'].min()):
        Unique_key['key'] = Unique_key['ItemCategoryCode']+"_"+Unique_key['CustomerGroupName']
        Unique_key = Unique_key.drop_duplicates()
        cs_y = rfm.copy(deep = True)
        cs_columns = cs_y.columns
        cs_columns = [x + "_" + str(i) for x in cs_columns] 
        cs_y.columns = cs_columns
        cs_y['key'] = cs_y['ItemCategoryCode_'+str(i)]+"_"+cs_y['CustomerGroupName_'+str(i)]
        cs_y = cs_y.drop(['ItemCategoryCode_'+str(i),'CustomerGroupName_'+str(i)], axis = 1)
        cs_y = Unique_key.merge(cs_y,on= 'key', how = 'left')
    else:
        cs_columns = rfm.columns
        cs_columns = [x + "_" + str(i) for x in cs_columns]
        rfm.columns = cs_columns
        rfm['key'] = rfm['ItemCategoryCode_'+str(i)]+"_"+rfm['CustomerGroupName_'+str(i)]
        rfm = rfm.drop(['ItemCategoryCode_'+str(i),'CustomerGroupName_'+str(i)],axis=1)
        cs_y = cs_y.merge(rfm,on='key',how='left')
rfm.to_csv(Stage2_Path+r"\ParquetData\Sales\\"+"Customer_Segmentation_yearly.csv")
cs_y.to_sql('customer_segmentation_yearly', engine,if_exists='replace',schema='sales')

Sales_Year = Sales_YTD.copy(deep = True)
Sales_Year['LinkDate'] = pd.to_datetime(Sales_Year['LinkDate'], format='%Y-%m-%d')
Sales_Year['Year'] = Sales_Year['LinkDate'].dt.year
for i in range(Sales_Year['Year'].min(),Sales_Year['Year'].max()+1):
    S_Temp = Sales_Year[(Sales_Year['Year'] <= i)]
    rfm = S_Temp.groupby(['ItemCategoryCode','CustomerGroupName']).agg({'LinkDate':'max','DocumentNo_':'count','InvoiceAt':'sum'})
    rfm.reset_index(inplace=True)
    rfm['max_date'] = rfm['LinkDate'].max()
    rfm['Days'] = (rfm['max_date'] - rfm['LinkDate']).dt.days
    rfm.rename(columns = {'Days': 'Recency','DocumentNo_': 'Frequency','InvoiceAt': 'Monetary'}, inplace=True)
    quintiles = rfm[['Recency', 'Frequency', 'Monetary']].quantile([.2, .4, .6, .8]).to_dict()
    
    rfm['R'] = rfm['Recency'].apply(lambda x: r_score(x))
    rfm['F'] = rfm['Frequency'].apply(lambda x: fm_score(x, 'Frequency'))
    rfm['M'] = rfm['Monetary'].apply(lambda x: fm_score(x, 'Monetary'))
    rfm['RFM'] = rfm['R'].astype(str)+rfm['F'].astype(str)+rfm['M'].astype(str)
    rfm = rfm.drop(['R','F','M'],axis=1)
    
    score['RFM'] = score['RFM'].astype(str)
    rfm = rfm.merge(score, on = 'RFM', how ='left')
    if(i == Sales_Year['Year'].min()):
        cs_y = rfm.copy(deep = True)
        cs_columns = cs_y.columns
        cs_columns = [x + "_" + str(i) for x in cs_columns] 
        cs_y.columns = cs_columns
        cs_y['key'] = cs_y['ItemCategoryCode_'+str(i)]+"_"+cs_y['CustomerGroupName_'+str(i)]
        cs_y = cs_y.drop(['ItemCategoryCode_'+str(i),'CustomerGroupName_'+str(i)], axis = 1)
        cs_y = Unique_key.merge(cs_y,on= 'key', how = 'left')
    else:
        cs_columns = rfm.columns
        cs_columns = [x + "_" + str(i) for x in cs_columns]
        rfm.columns = cs_columns
        rfm['key'] = rfm['ItemCategoryCode_'+str(i)]+"_"+rfm['CustomerGroupName_'+str(i)]
        rfm = rfm.drop(['ItemCategoryCode_'+str(i),'CustomerGroupName_'+str(i)],axis=1)
        cs_y = cs_y.merge(rfm,on='key',how='left')
rfm.to_csv(Stage2_Path+r"\ParquetData\Sales\\"+"Customer_Segmentation_ytd.csv")
cs_y.to_sql('customer_segmentation_ytd', engine,if_exists='replace', schema='sales')
