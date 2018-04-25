

import pandas as pd
import numpy as np
import sklearn
import os
import datetime as dt
import matplotlib.pyplot as plt
import pyodbc
import pandas.io.sql as psql
import seaborn as sns

# this is a test


# Setup connection to Azure
server = 'tcp:lettuce-predict.database.windows.net'
database = 'lettuce_predict' 
username = 'readuser@lettuce-predict'
password = 'rDuser1234!@#$' 

cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username + ';PWD='+password)



# # Load Data from Azure
sql =""" 
SELECT id, date, store_nbr, item_nbr, onpromotion, unit_sales, 'train' as data_set FROM train
where date >='01-JAN-2016'

"""
train = psql.read_sql(sql, cnxn)

sql =""" 
SELECT id, date, store_nbr, item_nbr, onpromotion, 0 as unit_sales, 'test' as data_set FROM test
where date >='01-JAN-2016'
"""
test = psql.read_sql(sql, cnxn)

sql =""" 
SELECT * FROM items
"""
items = psql.read_sql(sql, cnxn)

sql =""" 
SELECT * FROM holidays_events
"""
holidays_events = psql.read_sql(sql, cnxn)

sql =""" 
SELECT * FROM oil
"""
oil = psql.read_sql(sql, cnxn)

sql =""" 
SELECT * FROM stores
"""
stores = psql.read_sql(sql, cnxn)

sql =""" 
SELECT * FROM transactions

"""
transactions = psql.read_sql(sql, cnxn)



# Get files current directory
#path = r"C:\Users\jeffrey.tackes\Desktop\MSDS\Data"

# Aggregate train & test sets
df = pd.concat([test,train])
del train
del test


# Merge Data 
df1 = pd.merge(df, transactions, on = ['date','store_nbr'], how = 'left')
del df
df1 = pd.merge(df1, stores, on = 'store_nbr', how = 'left')
df1 = pd.merge(df1, oil, on = 'date', how = 'left')
df1 = pd.merge(df1, items, on = 'item_nbr', how = 'left')

# Fill Missing Promotion Data
df1['onpromotion'] = df1['onpromotion'].fillna(0)

# Holidays are useless per the 1st place winning solution
#holiday = holidays_events.loc[holidays_events['transferred'] == False]
#df1 = pd.merge(df1, holidays_events, on = 'date', how = 'left')

# Memory Optimization
mem_test=df1.memory_usage(index=True).sum()
print("test dataset uses ",mem_test/ 1024**2," MB")

#There are only 54 stores
df1['store_nbr'] = df1['store_nbr'].astype(np.uint8)
# The ID column is a continuous number from 1 to 128867502 in train and 128867503 to 125497040 in test
df1['id'] = df1['id'].astype(np.uint32)
# item number is unsigned 
df1['item_nbr'] = df1['item_nbr'].astype(np.uint32)
#Converting the date column to date format
df1['date']=pd.to_datetime(df1['date'],format="%Y-%m-%d")

#check memory
print(df1.memory_usage(index=True))
new_mem_test=df1.memory_usage(index=True).sum()
print("test dataset uses ",new_mem_test/ 1024**2," MB after changes")
print("memory saved =",(mem_test-new_mem_test)/ 1024**2," MB")

print("Memory used by the ID field: ",df1.id.memory_usage(index=False) /1024**2 , " MB" )
print("Memory used by the date field: ",df1.date.memory_usage(index=False) /1024**2 , " MB" )
print("Memory used by the store_nbr field: ",df1.store_nbr.memory_usage(index=False) /1024**2 , " MB" )
print("Memory used by the item_nbr field: ",df1.item_nbr.memory_usage(index=False) /1024**2 , " MB" )
print("Memory used by the onpromotion field: ",df1.onpromotion.memory_usage(index=False) /1024**2 , " MB" )
print("Memory used by the unit_sales field: ",df1.unit_sales.memory_usage(index=False) /1024**2 , " MB" )
print("Memory used by the data_set field: ",df1.data_set.memory_usage(index=False) /1024**2 , " MB" )
print("Memory used by the transactions field: ",df1.transactions.memory_usage(index=False) /1024**2 , " MB" )
print("Memory used by the city field: ",df1.city.memory_usage(index=False) /1024**2 , " MB" )
print("Memory used by the state field: ",df1.state.memory_usage(index=False) /1024**2 , " MB" )
print("Memory used by the type field: ",df1.type.memory_usage(index=False) /1024**2 , " MB" )
print("Memory used by the cluster field: ",df1.cluster.memory_usage(index=False) /1024**2 , " MB" )
print("Memory used by the dcoilwtico field: ",df1.dcoilwtico.memory_usage(index=False) /1024**2 , " MB" )
print("Memory used by the family field: ",df1.family.memory_usage(index=False) /1024**2 , " MB" )
print("Memory used by the class field: ",df1['class'].memory_usage(index=False) /1024**2 , " MB" )
print("Memory used by the perishable field: ",df1.perishable.memory_usage(index=False) /1024**2 , " MB" )






#  Data Engineerings
df1['date'] = pd.to_datetime(df1['date'])
df1['dayofweek'] = df1['date'].dt.weekday_name # {0:'Mon',1:'Tues',2:'Weds',3:'Thurs',4:'Fri',5:'Sat',6:'Sun'}
df1['day'] = df1['date'].dt.day
df1['month'] = df1['date'].dt.month
df1['year'] = df1['date'].dt.year

#df1.groupby([df1.family]).count().plot(kind='bar')
print("Unique Stores: " , df1.state.unique() )

df1.shape

df1.groupby(['dayofweek'])['unit_sales'].sum().plot(kind='bar')
plt.title('Total Unit Sales by Day of Week')
plt.show()

df1.groupby(['month'])['unit_sales'].sum().plot(kind='bar')
plt.title('Total Unit Sales by Month')
plt.show()

df1.date.min()
df1.date.max()
df1.columns

df1.head()


plt.plot(df1.date, df1.unit_sales)
plt.legend('type', ncol = 2, loc = 'upper left')


pv = pd.pivot_table(df1, index=df1.date, columns=df1.type,
                    values='unit_sales', aggfunc='sum')
pv.plot()
plt.title('Total Unit Sales by Store Type')
#plt.legend(df1.type, ncol = 2, loc = 'upper left')


pv = pd.pivot_table(df1, index=df1.date, columns=df1.type,
                    values='transactions', aggfunc='sum')
pv.plot()
plt.title('Total Transactions by Store Type')


df1.groupby(['onpromotion'])['transactions'].sum().plot(kind='bar')
plt.title('On Promotion Sales')
plt.show()

df1[df1.unit_sales < 0].groupby(['date'])['unit_sales'].sum().plot(kind='bar')
plt.title('Negative sales = Returns')
plt.show()

pv = pd.pivot_table(df1[df1.unit_sales < 0], index='date', columns='type',
                    values='unit_sales', aggfunc='sum')
pv.plot()


fig, axes = plt.subplots(nrows=5, ncols=2,figsize=(15,15))
df1.groupby(['type'])['unit_sales'].sum().plot(kind='bar', ax = axes[0,0])
df1.groupby(['type'])['transactions'].sum().plot(kind='bar', ax = axes[0,1])
df1.groupby(['cluster'])['unit_sales'].sum().plot(kind='bar', ax = axes[1,0])
df1.groupby(['cluster'])['transactions'].sum().plot(kind='bar', ax = axes[1,1])
df1.groupby(['city'])['unit_sales'].sum().plot(kind='bar', ax = axes[2,0])
df1.groupby(['city'])['transactions'].sum().plot(kind='bar', ax = axes[2,1])
df1.groupby(['state'])['unit_sales'].sum().plot(kind='bar', ax = axes[3,0])
df1.groupby(['state'])['transactions'].sum().plot(kind='bar', ax = axes[3,1])
df1.groupby(['family'])['unit_sales'].sum().plot(kind='bar', ax = axes[4,0])
df1.groupby(['family'])['transactions'].sum().plot(kind='bar', ax = axes[4,1])
axes[0,0].set_ylabel('total unit_sales')
axes[1,0].set_ylabel('total unit_sales')
axes[2,0].set_ylabel('total unit_sales')
axes[3,0].set_ylabel('total unit_sales')
axes[0,1].set_ylabel('total transactions')
axes[1,1].set_ylabel('total transactions')
axes[2,1].set_ylabel('total transactions')
axes[3,1].set_ylabel('total transactions')
axes[4,0].set_ylabel('total unit_sales')
axes[4,1].set_ylabel('total transactions')


pv = pd.pivot_table(oil, index='date', 
                    values='dcoilwtico', aggfunc='sum')

pv.plot()
plt.title('Oil Price in USD')


