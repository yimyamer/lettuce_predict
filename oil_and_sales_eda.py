# Load Packages
import pandas as pd
import numpy as np
import sklearn
import os
import datetime as dt
import matplotlib.pyplot as plt
import pandas.io.sql as psql
import pyodbc

# Setup connection to Azure
server = 'tcp:lettuce-predict.database.windows.net'
database = 'lettuce_predict'
username = 'readuser@lettuce-predict'
password = 'rDuser1234!@#$'
driver= '{ODBC Driver 13 for SQL Server}'

cnxn = pyodbc.connect('DRIVER='+driver+';PORT=1433;SERVER='+server+';DATABASE='+database+';UID='+username + ';PWD='+password)

# Import daily oil prices

sql = """ 
SELECT * 
FROM oil
WHERE date BETWEEN '2016-01-01' and '2017-06-30'
"""
oil = psql.read_sql(sql, cnxn)
#oil.head(5)

# Impute missing values with rolling average

oil_i = oil.fillna(oil.rolling(4,min_periods=1).mean())
#oil_i.head(25)

# Import daily volume grouped by store

sql = """
SELECT t.date, SUM(t.transactions) as transactions 
FROM dbo.transactions t
WHERE t.date BETWEEN '2016-01-01' and '2017-06-30'
GROUP BY t.date;
"""

sales = psql.read_sql(sql, cnxn)
#sales.head(3)

# Join Oil and Sales dataframes

oil_sales = oil_i.set_index('date').join(sales.set_index('date'))
#oil_sales.head(25)

# Plots

plt.plot(oil_sales.index, oil_sales.dcoilwtico)
plt.show()

fig, ax1 = plt.subplots()

x = oil_sales.index
y1 = oil_sales.dcoilwtico
y2 = np.log(oil_sales.transactions)

ax2 = ax1.twinx()
ax1.plot(x, y1, 'g-')
ax2.plot(x, y2, 'b-')
ax1.set_xlabel('date')
ax1.set_ylabel('oil price', color='g')
ax2.set_ylabel('log(transactions)', color='b')
plt.show()

# Import daily sales by item family

by_family = pd.read_csv('~/desktop/Northwestern/by_family.csv', sep=',', header='infer')

# Impute NaN values
by_family_i = by_family.fillna(by_family.rolling(4,min_periods=1).mean())
#by_family_i.head(3)
# Convert to DateTime
by_family_i['date'] = pd.to_datetime(by_family_i['date'])
# Resample to weekly
weekly_family = by_family_i.reset_index().set_index('date').resample('w').sum()

#weekly_family.head(3)
#plt.plot(by_family_i['date'], np.log(by_family_i['DAIRY']))
#plt.show()

# Convert Oil to weekly
oil['date'] = pd.to_datetime(oil['date'])
weekly_oil = oil.reset_index().set_index('date').resample('w').mean()
#weekly_oil.head(3)

# Convert Sales to weekly
sales['date'] = pd.to_datetime(sales['date'])
weekly_sales = sales.reset_index().set_index('date').resample('w').sum()
#weekly_oil.head(3)

####### Plot time series #######

# Trim to improve visual display[
wk_sales = weekly_sales.iloc[2:-1]
wk_oil = weekly_oil.iloc[2:-1]

x1 = wk_oil.index
x2 = wk_sales.index
y1 = wk_oil.dcoilwtico
y2 = wk_sales.transactions

plt.subplot(2, 1, 1)
plt.plot(x1, y1, color='black')
plt.title('Weekly Average Oil Price & Total Grocery Sales')
plt.ylabel('Average Oil Price')

plt.subplot(2, 1, 2)
plt.plot(x2, y2, color='green')
plt.xlabel('week')
plt.ylabel('Total Sales')

plt.show()

# Convert weekly Sales and Oil to % change

w_sales = weekly_sales.pct_change()
w_oil = weekly_oil.pct_change()

# Trim to improve visual display
w_sales = w_sales.iloc[2:-1]
w_oil = w_oil.iloc[2:-1]

# Plot Weekly % Change Oil and Transactions

x1 = w_oil.index
x2 = w_sales.index
y1 = w_oil.dcoilwtico
y2 = w_sales.transactions

plt.subplot(2, 1, 1)
plt.plot(x1, y1, color='black')
plt.title('% Weekly Change Oil Price & Grocery Sales')
plt.ylabel('% Change Oil Price')

plt.subplot(2, 1, 2)
plt.plot(x2, y2, color='green')
plt.xlabel('week')
plt.ylabel('% Change Sales')

plt.show()

# Correlation
y1.corr(y2)
# - 0.1711

# BY FAMILY

#create % Change
w_family = weekly_family.pct_change()
#trim
w_family = w_family.iloc[2:-1]
#w_family.head(3)

# Calculation Pearson's Score for w_oil and weekly categories

y1.corr(w_family['AUTOMOTIVE'])
y1.corr(w_family['BABY CARE'])
y1.corr(w_family['BEAUTY'])
y1.corr(w_family['BEVERAGES'])
y1.corr(w_family['BOOKS'])
y1.corr(w_family['BREAD/BAKERY'])
y1.corr(w_family['CELEBRATION'])
y1.corr(w_family['CLEANING'])
y1.corr(w_family['DAIRY'])
y1.corr(w_family['DELI'])
y1.corr(w_family['EGGS'])
y1.corr(w_family['FROZEN FOODS'])
y1.corr(w_family['GROCERY I'])
y1.corr(w_family['GROCERY II'])
y1.corr(w_family['HARDWARE'])
y1.corr(w_family['HOME AND KITCHEN I'])
y1.corr(w_family['HOME AND KITCHEN II'])
y1.corr(w_family['HOME APPLIANCES'])
y1.corr(w_family['HOME CARE'])
y1.corr(w_family['LADIESWEAR'])
y1.corr(w_family['LAWN AND GARDEN'])
y1.corr(w_family['LINGERIE'])
y1.corr(w_family['LIQUOR,WINE,BEER'])
y1.corr(w_family['MAGAZINES'])
y1.corr(w_family['MEATS'])
y1.corr(w_family['PERSONAL CARE'])
y1.corr(w_family['PET SUPPLIES'])
y1.corr(w_family['PLAYERS AND ELECTRONICS'])
y1.corr(w_family['POULTRY'])
y1.corr(w_family['PREPARED FOODS'])
y1.corr(w_family['PRODUCE'])
y1.corr(w_family['SCHOOL AND OFFICE SUPPLIES'])
y1.corr(w_family['SEAFOOD'])

# Explore Impact of Earthquake (April, 16 2016)

sql = """
SELECT t.date, SUM(t.transactions) as transactions 
FROM dbo.transactions t
WHERE t.date BETWEEN '2016-03-01' and '2016-06-30'
GROUP BY t.date;
"""

earthquake = psql.read_sql(sql, cnxn)
earthquake.head(3)

#plot daily Q2 2016 w/ red line as date of earthquake
plt.plot(earthquake.date,earthquake.transactions)
plt.axvline(dt.datetime(2016, 4, 16), color='r')
plt.show()

#convert to weekly
earthquake_w = earthquake.copy()
earthquake_w['date'] = pd.to_datetime(earthquake_w['date'])
earthquake_w = earthquake_w.reset_index().set_index('date').resample('w').sum()
earthquake_w.head(3)

#plot weekly Q2 2016 w/ red line indicating date of earthquake
plt.plot(earthquake_w.index,earthquake_w.transactions)
plt.axvline(dt.datetime(2016, 4, 16), color='r')
plt.show()

