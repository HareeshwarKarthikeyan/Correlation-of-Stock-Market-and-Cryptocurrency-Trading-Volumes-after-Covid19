# ANALYSIS TASK 2
# How similar is the directional nature (positive and negative) of the changes in the price and volume in the markets before and after covid hit?

from pyspark import SparkContext
from pyspark.sql import SQLContext
import pandas as pd
sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)
data_main = sqlContext.read.csv('data.csv',header=False)
data_main.show(5)
# Order of columns
# 'DATE', 'C_PCP', 'C_TMVCP', 'N_PCP', 'N_TMVCP', 'SP_PCP', 'SP_TMVCP', 'NT_PCP', 'NT_TMVCP'

data_main = data_main.rdd.map(lambda x: (x[0],float(x[1]),float(x[2]),float(x[3]),float(x[4]),float(x[5]),float(x[6]),float(x[7]),float(x[8])) )

# Before covid
from datetime import datetime
def reformat_date(date_time_str):
    dateitem = datetime.strptime(date_time_str, '%d-%m-%Y')
    return dateitem
date = datetime.strptime('31-12-2019', '%d-%m-%Y')
data1 = data_main.filter(lambda x: reformat_date(x[0])<date)
data1.count()

data_df = data1.toDF(('DATE', 'C_PCP', 'C_TMVCP', 'N_PCP', 'N_TMVCP', 'SP_PCP', 'SP_TMVCP', 'NT_PCP', 'NT_TMVCP'))
print('Comparing daily avg price change percentage across the indices')
print('Crypto vs Nasdaq100')
pp = data_df.rdd.filter(lambda x: (x[1]>0 and x[3]>0) or (x[1]==0 and x[3]==0)).count()
nn = data_df.rdd.filter(lambda x: (x[1]<0 and x[3]<0)).count()
pn = data_df.rdd.filter(lambda x: (x[1]>0 and x[3]<0)).count()
np = data_df.rdd.filter(lambda x: (x[1]<0 and x[3]>0)).count()
data = [[pp,pn],[np,nn]]
columns = ['nasdaq100_positive','nasdaq100_negative']
indices = ['crypto_positive','crypto_negative']
df = pd.DataFrame(data,indices,columns)
print(df)
print('Percentage of records having the same direction of price change: ' + str(100*(pp+nn)/(pp+pn+np+nn)))

print('Crypto vs S&P100')
pp = data_df.rdd.filter(lambda x: (x[1]>0 and x[5]>0) or (x[1]==0 and x[5]==0)).count()
nn = data_df.rdd.filter(lambda x: (x[1]<0 and x[5]<0)).count()
pn = data_df.rdd.filter(lambda x: (x[1]>0 and x[5]<0)).count()
np = data_df.rdd.filter(lambda x: (x[1]<0 and x[5]>0)).count()
data = [[pp,pn],[np,nn]]
columns = ['s&p100_positive','s&p100_negative']
indices = ['crypto_positive','crypto_negative']
df = pd.DataFrame(data,indices,columns)
print(df)
print('Percentage of records having the same direction of price change: ' + str(100*(pp+nn)/(pp+pn+np+nn)))

print('Crypto vs Nasdaq100Tech')
pp = data_df.rdd.filter(lambda x: (x[1]>0 and x[7]>0) or (x[1]==0 and x[7]==0)).count()
nn = data_df.rdd.filter(lambda x: (x[1]<0 and x[7]<0)).count()
pn = data_df.rdd.filter(lambda x: (x[1]>0 and x[7]<0)).count()
np = data_df.rdd.filter(lambda x: (x[1]<0 and x[7]>0)).count()
data = [[pp,pn],[np,nn]]
columns = ['nasdaq100Tech_positive','nasdaq100Tech_negative']
indices = ['crypto_positive','crypto_negative']
df = pd.DataFrame(data,indices,columns)
print(df)
print('Percentage of records having the same direction of price change: ' + str(100*(pp+nn)/(pp+pn+np+nn)))

print('Comparing daily avg traded monetary volume change percentage across the indices')
print('Crypto vs Nasdaq100')
pp = data_df.rdd.filter(lambda x: (x[2]>0 and x[4]>0) or (x[2]==0 and x[4]==0)).count()
nn = data_df.rdd.filter(lambda x: (x[2]<0 and x[4]<0)).count()
pn = data_df.rdd.filter(lambda x: (x[2]>0 and x[4]<0)).count()
np = data_df.rdd.filter(lambda x: (x[2]<0 and x[4]>0)).count()
data = [[pp,pn],[np,nn]]
columns = ['nasdaq100_positive','nasdaq100_negative']
indices = ['crypto_positive','crypto_negative']
df = pd.DataFrame(data,indices,columns)
print(df)
print('Percentage of records having the same direction of tmv change: ' + str(100*(pp+nn)/(pp+pn+np+nn)))

print('Crypto vs S&P100')
pp = data_df.rdd.filter(lambda x: (x[2]>0 and x[6]>0) or (x[2]==0 and x[6]==0)).count()
nn = data_df.rdd.filter(lambda x: (x[2]<0 and x[6]<0)).count()
pn = data_df.rdd.filter(lambda x: (x[2]>0 and x[6]<0)).count()
np = data_df.rdd.filter(lambda x: (x[2]<0 and x[6]>0)).count()
data = [[pp,pn],[np,nn]]
columns = ['s&p100_positive','s&p100_negative']
indices = ['crypto_positive','crypto_negative']
df = pd.DataFrame(data,indices,columns)
print(df)
print('Percentage of records having the same direction of tmv change: ' + str(100*(pp+nn)/(pp+pn+np+nn)))

print('Crypto vs Nasdaq100Tech')
pp = data_df.rdd.filter(lambda x: (x[2]>0 and x[8]>0) or (x[2]==0 and x[8]==0)).count()
nn = data_df.rdd.filter(lambda x: (x[2]<0 and x[8]<0)).count()
pn = data_df.rdd.filter(lambda x: (x[2]>0 and x[8]<0)).count()
np = data_df.rdd.filter(lambda x: (x[2]<0 and x[8]>0)).count()
data = [[pp,pn],[np,nn]]
columns = ['nasdaq100Tech_positive','nasdaq100Tech_negative']
indices = ['crypto_positive','crypto_negative']
df = pd.DataFrame(data,indices,columns)
print(df)
print('Percentage of records having the same direction of tmv change: ' + str(100*(pp+nn)/(pp+pn+np+nn)))

# After covid
date = datetime.strptime('31-12-2019', '%d-%m-%Y')
data2 = data_main.filter(lambda x: reformat_date(x[0])>=date)
data2.count()

data_df = data2.toDF(('DATE', 'C_PCP', 'C_TMVCP', 'N_PCP', 'N_TMVCP', 'SP_PCP', 'SP_TMVCP', 'NT_PCP', 'NT_TMVCP'))
print('Comparing daily avg price change percentage across the indices')
print('Crypto vs Nasdaq100')
pp = data_df.rdd.filter(lambda x: (x[1]>0 and x[3]>0) or (x[1]==0 and x[3]==0)).count()
nn = data_df.rdd.filter(lambda x: (x[1]<0 and x[3]<0)).count()
pn = data_df.rdd.filter(lambda x: (x[1]>0 and x[3]<0)).count()
np = data_df.rdd.filter(lambda x: (x[1]<0 and x[3]>0)).count()
data = [[pp,pn],[np,nn]]
columns = ['nasdaq100_positive','nasdaq100_negative']
indices = ['crypto_positive','crypto_negative']
df = pd.DataFrame(data,indices,columns)
print(df)
print('Percentage of records having the same direction of price change: ' + str(100*(pp+nn)/(pp+pn+np+nn)))

print('Crypto vs S&P100')
pp = data_df.rdd.filter(lambda x: (x[1]>0 and x[5]>0) or (x[1]==0 and x[5]==0)).count()
nn = data_df.rdd.filter(lambda x: (x[1]<0 and x[5]<0)).count()
pn = data_df.rdd.filter(lambda x: (x[1]>0 and x[5]<0)).count()
np = data_df.rdd.filter(lambda x: (x[1]<0 and x[5]>0)).count()
data = [[pp,pn],[np,nn]]
columns = ['s&p100_positive','s&p100_negative']
indices = ['crypto_positive','crypto_negative']
df = pd.DataFrame(data,indices,columns)
print(df)
print('Percentage of records having the same direction of price change: ' + str(100*(pp+nn)/(pp+pn+np+nn)))

print('Crypto vs Nasdaq100Tech')
pp = data_df.rdd.filter(lambda x: (x[1]>0 and x[7]>0) or (x[1]==0 and x[7]==0)).count()
nn = data_df.rdd.filter(lambda x: (x[1]<0 and x[7]<0)).count()
pn = data_df.rdd.filter(lambda x: (x[1]>0 and x[7]<0)).count()
np = data_df.rdd.filter(lambda x: (x[1]<0 and x[7]>0)).count()
data = [[pp,pn],[np,nn]]
columns = ['nasdaq100Tech_positive','nasdaq100Tech_negative']
indices = ['crypto_positive','crypto_negative']
df = pd.DataFrame(data,indices,columns)
print(df)
print('Percentage of records having the same direction of price change: ' + str(100*(pp+nn)/(pp+pn+np+nn)))

print('Comparing daily avg traded monetary volume change percentage across the indices')
print('Crypto vs Nasdaq100')
pp = data_df.rdd.filter(lambda x: (x[2]>0 and x[4]>0) or (x[2]==0 and x[4]==0)).count()
nn = data_df.rdd.filter(lambda x: (x[2]<0 and x[4]<0)).count()
pn = data_df.rdd.filter(lambda x: (x[2]>0 and x[4]<0)).count()
np = data_df.rdd.filter(lambda x: (x[2]<0 and x[4]>0)).count()
data = [[pp,pn],[np,nn]]
columns = ['nasdaq100_positive','nasdaq100_negative']
indices = ['crypto_positive','crypto_negative']
df = pd.DataFrame(data,indices,columns)
print(df)
print('Percentage of records having the same direction of tmv change: ' + str(100*(pp+nn)/(pp+pn+np+nn)))

print('Crypto vs S&P100')
pp = data_df.rdd.filter(lambda x: (x[2]>0 and x[6]>0) or (x[2]==0 and x[6]==0)).count()
nn = data_df.rdd.filter(lambda x: (x[2]<0 and x[6]<0)).count()
pn = data_df.rdd.filter(lambda x: (x[2]>0 and x[6]<0)).count()
np = data_df.rdd.filter(lambda x: (x[2]<0 and x[6]>0)).count()
data = [[pp,pn],[np,nn]]
columns = ['s&p100_positive','s&p100_negative']
indices = ['crypto_positive','crypto_negative']
df = pd.DataFrame(data,indices,columns)
print(df)
print('Percentage of records having the same direction of tmv change: ' + str(100*(pp+nn)/(pp+pn+np+nn)))

print('Crypto vs Nasdaq100Tech')
pp = data_df.rdd.filter(lambda x: (x[2]>0 and x[8]>0) or (x[2]==0 and x[8]==0)).count()
nn = data_df.rdd.filter(lambda x: (x[2]<0 and x[8]<0)).count()
pn = data_df.rdd.filter(lambda x: (x[2]>0 and x[8]<0)).count()
np = data_df.rdd.filter(lambda x: (x[2]<0 and x[8]>0)).count()
import pandas as pd
data = [[pp,pn],[np,nn]]
columns = ['nasdaq100Tech_positive','nasdaq100Tech_negative']
indices = ['crypto_positive','crypto_negative']
df = pd.DataFrame(data,indices,columns)
print(df)
print('Percentage of records having the same direction of tmv change: ' + str(100*(pp+nn)/(pp+pn+np+nn)))


# no big change after covid
# small increases in some
# small decreases in some