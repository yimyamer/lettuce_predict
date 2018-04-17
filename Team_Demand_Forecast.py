

import pandas as pd
import numpy as np
import sklearn
import os
import datetime as dt
import matplotlib.pyplot as plt
import pyodbc
import pandas.io.sql as psql

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
where date >='01-JAN-2017'

"""
train = psql.read_sql(sql, cnxn)

sql =""" 
SELECT id, date, store_nbr, item_nbr, onpromotion, -1 as unit_sales, 'test' as data_set FROM test
where date >='01-JAN-2017'
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
df1 = pd.merge(df, items, on = 'item_nbr', how = 'left')
del df
df1 = pd.merge(df1, holidays_events, on = 'date', how = 'left')
df1 = pd.merge(df1, oil, on = 'date', how = 'left')
df1 = pd.merge(df1, stores, on = 'store_nbr', how = 'left')
df1 = pd.merge(df1, transactions, on = ['date','store_nbr'], how = 'left')

#  Data Engineerings
df1['date'] = pd.to_datetime(df1['date'])
df1['dayofweek'] = df1['date'].dt.weekday_name # {0:'Mon',1:'Tues',2:'Weds',3:'Thurs',4:'Fri',5:'Sat',6:'Sun'}
df1['day'] = df1['date'].dt.day
df1['month'] = df1['date'].dt.month
df1['year'] = df1['date'].dt.year

df1.groupby([df1.family]).count().plot(kind='bar')
print("Unique Stores: " , df1.state.unique() )

df1.shape

df1.groupby(['dayofweek'])['unit_sales'].sum().plot(kind='bar')
df1.groupby(['month'])['unit_sales'].sum().plot(kind='bar')

df1.date.min()
df1.date.max()
df1.columns

df1.head()








