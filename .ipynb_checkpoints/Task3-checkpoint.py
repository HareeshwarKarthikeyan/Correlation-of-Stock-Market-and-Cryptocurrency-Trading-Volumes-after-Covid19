# Original by Hareeshwar Karthikeyan

# ANALYSIS TASK 3
#  Is there any correlation in the time series data to see if the crypto trends reflect the stock trends in price and volume changes?

from pyspark import SparkContext
from pyspark.sql import SQLContext
sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)
data_main = sqlContext.read.csv('data.csv',header=False)

# Adding an index column
dates = data_main.rdd.map(lambda x: x[0]).collect()
# Order of columns
# 'INDEX','DATE', 'C_PCP', 'C_TMVCP', 'N_PCP', 'N_TMVCP', 'SP_PCP', 'SP_TMVCP', 'NT_PCP', 'NT_TMVCP'
data_main = data_main.rdd.map(lambda x: (dates.index(x[0]),x[0],float(x[1]),float(x[2]),float(x[3]),float(x[4]),float(x[5]),float(x[6]),float(x[7]),float(x[8])) )
data_main.toDF().show(5)

# Before covid
from datetime import datetime
def reformat_date(date_time_str):
    dateitem = datetime.strptime(date_time_str, '%d-%m-%Y')
    return dateitem

date = datetime.strptime('31-12-2019', '%d-%m-%Y')
data_bc = data_main.filter(lambda x: reformat_date(x[1])<date)
bc_correlations = []

# cross correlation - time lagged
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# price change percent
# crypto and nasdaq100
data = data_bc.map(lambda x: (x[0],x[1],x[4],x[2]))
# print(data.count())
data_df = data.toDF(('INDEX','DATE','N_PCP','C_PCP'))
data_df.registerTempTable('TABLE')
# we are trying to study the effect of N_PCP change trends on C_PCP change trends
# creating time lagged values for C_PCP for upto 30 days
columns = []
columns.append('C_PCP')
cross_correlations = []
data_dfx = data_df
for i in range(1,31):
    window = Window.orderBy("Index")
    leadCol = F.lead("C_PCP", i).over(window)
    columns.append("C_PCP+"+str(i))
    data_dfx = data_dfx.withColumn("C_PCP+"+str(i), leadCol)
    data_dfx.registerTempTable('DATA')
for column in columns:
    query = sqlContext.sql('SELECT CORR(N_PCP,{0}) as CORRELATION FROM DATA'.format(column))
    cross_correlations.append(query.select('CORRELATION').collect()[0].CORRELATION)
#print(cross_correlations)
bc_correlations.append(cross_correlations)

# price change percent
# crypto and sp100
data = data_bc.map(lambda x: (x[0],x[1],x[6],x[2]))
data_df = data.toDF(('INDEX','DATE','SP_PCP','C_PCP'))
data_df.registerTempTable('TABLE')
# we are trying to study the effect of SP_PCP change trends on C_PCP change trends
# creating time lagged values for C_PCP for upto 30 days
columns = []
columns.append('C_PCP')
cross_correlations = []
data_dfx = data_df
for i in range(1,31):
    window = Window.orderBy("Index")
    leadCol = F.lead("C_PCP", i).over(window)
    columns.append("C_PCP+"+str(i))
    data_dfx = data_dfx.withColumn("C_PCP+"+str(i), leadCol)
    data_dfx.registerTempTable('DATA')
for column in columns:
    query = sqlContext.sql('SELECT CORR(SP_PCP,{0}) as CORRELATION FROM DATA'.format(column))
    cross_correlations.append(query.select('CORRELATION').collect()[0].CORRELATION)
#print(cross_correlations)
bc_correlations.append(cross_correlations)

# price change percent
# crypto and Nasdaq100Tech
data = data_bc.map(lambda x: (x[0],x[1],x[8],x[2]))
data_df = data.toDF(('INDEX','DATE','NT_PCP','C_PCP'))
data_df.registerTempTable('TABLE')
# we are trying to study the effect of NT_PCP change trends on C_PCP change trends
# creating time lagged values for C_PCP for upto 30 days
columns = []
columns.append('C_PCP')
cross_correlations = []
data_dfx = data_df
for i in range(1,31):
    window = Window.orderBy("Index")
    leadCol = F.lead("C_PCP", i).over(window)
    columns.append("C_PCP+"+str(i))
    data_dfx = data_dfx.withColumn("C_PCP+"+str(i), leadCol)
    data_dfx.registerTempTable('DATA')
for column in columns:
    query = sqlContext.sql('SELECT CORR(NT_PCP,{0}) as CORRELATION FROM DATA'.format(column))
    cross_correlations.append(query.select('CORRELATION').collect()[0].CORRELATION)
#print(cross_correlations)
bc_correlations.append(cross_correlations)

# tmv change percent
# crypto and nasdaq100
data = data_bc.map(lambda x: (x[0],x[1],x[5],x[3]))
data_df = data.toDF(('INDEX','DATE','N_TMVCP','C_TMVCP'))
data_df.registerTempTable('TABLE')
# we are trying to study the effect of N_TMVCP change trends on C_TMVCP change trends
# creating time lagged values for C_TMVCP for upto 30 days
columns = []
columns.append('C_TMVCP')
cross_correlations = []
data_dfx = data_df
for i in range(1,31):
    window = Window.orderBy("Index")
    leadCol = F.lead("C_TMVCP", i).over(window)
    columns.append("C_TMVCP+"+str(i))
    data_dfx = data_dfx.withColumn("C_TMVCP+"+str(i), leadCol)
    data_dfx.registerTempTable('DATA')
for column in columns:
    query = sqlContext.sql('SELECT CORR(N_TMVCP,{0}) as CORRELATION FROM DATA'.format(column))
    cross_correlations.append(query.select('CORRELATION').collect()[0].CORRELATION)
#print(cross_correlations)
bc_correlations.append(cross_correlations)

# tmv change percent
# crypto and sp100
data = data_bc.map(lambda x: (x[0],x[1],x[7],x[3]))
data_df = data.toDF(('INDEX','DATE','SP_TMVCP','C_TMVCP'))
data_df.registerTempTable('TABLE')
# we are trying to study the effect of N_TMVCP change trends on C_TMVCP change trends
# creating time lagged values for C_TMVCP for upto 30 days
columns = []
columns.append('C_TMVCP')
cross_correlations = []
data_dfx = data_df
for i in range(1,31):
    window = Window.orderBy("Index")
    leadCol = F.lead("C_TMVCP", i).over(window)
    columns.append("C_TMVCP+"+str(i))
    data_dfx = data_dfx.withColumn("C_TMVCP+"+str(i), leadCol)
    data_dfx.registerTempTable('DATA')
for column in columns:
    query = sqlContext.sql('SELECT CORR(SP_TMVCP,{0}) as CORRELATION FROM DATA'.format(column))
    cross_correlations.append(query.select('CORRELATION').collect()[0].CORRELATION)
#print(cross_correlations)
bc_correlations.append(cross_correlations)

# tmv change percent
# crypto and Nadaq100Tech
data = data_bc.map(lambda x: (x[0],x[1],x[9],x[3]))
data_df = data.toDF(('INDEX','DATE','NT_TMVCP','C_TMVCP'))
data_df.registerTempTable('TABLE')
# we are trying to study the effect of N_TMVCP change trends on C_TMVCP change trends
# creating time lagged values for C_TMVCP for upto 30 days
columns = []
columns.append('C_TMVCP')
cross_correlations = []
data_dfx = data_df
for i in range(1,31):
    window = Window.orderBy("Index")
    leadCol = F.lead("C_TMVCP", i).over(window)
    columns.append("C_TMVCP+"+str(i))
    data_dfx = data_dfx.withColumn("C_TMVCP+"+str(i), leadCol)
    data_dfx.registerTempTable('DATA')
for column in columns:
    query = sqlContext.sql('SELECT CORR(NT_TMVCP,{0}) as CORRELATION FROM DATA'.format(column))
    cross_correlations.append(query.select('CORRELATION').collect()[0].CORRELATION)
#print(cross_correlations)
bc_correlations.append(cross_correlations)

# AFTER COVID
te = datetime.strptime('31-12-2019', '%d-%m-%Y')
data_ac = data_main.filter(lambda x: reformat_date(x[1])>=date)
ac_correlations = []

# cross correlation - time lagged
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# price change percent
# crypto and nasdaq100
data = data_ac.map(lambda x: (x[0],x[1],x[4],x[2]))
# print(data.count())
data_df = data.toDF(('INDEX','DATE','N_PCP','C_PCP'))
data_df.registerTempTable('TABLE')
# we are trying to study the effect of N_PCP change trends on C_PCP change trends
# creating time lagged values for C_PCP for upto 30 days
columns = []
columns.append('C_PCP')
cross_correlations = []
data_dfx = data_df
for i in range(1,31):
    window = Window.orderBy("Index")
    leadCol = F.lead("C_PCP", i).over(window)
    columns.append("C_PCP+"+str(i))
    data_dfx = data_dfx.withColumn("C_PCP+"+str(i), leadCol)
    data_dfx.registerTempTable('DATA')
for column in columns:
    query = sqlContext.sql('SELECT CORR(N_PCP,{0}) as CORRELATION FROM DATA'.format(column))
    cross_correlations.append(query.select('CORRELATION').collect()[0].CORRELATION)
#print(cross_correlations)
ac_correlations.append(cross_correlations)

# price change percent
# crypto and sp100
data = data_ac.map(lambda x: (x[0],x[1],x[6],x[2]))
data_df = data.toDF(('INDEX','DATE','SP_PCP','C_PCP'))
data_df.registerTempTable('TABLE')
# we are trying to study the effect of SP_PCP change trends on C_PCP change trends
# creating time lagged values for C_PCP for upto 30 days
columns = []
columns.append('C_PCP')
cross_correlations = []
data_dfx = data_df
for i in range(1,31):
    window = Window.orderBy("Index")
    leadCol = F.lead("C_PCP", i).over(window)
    columns.append("C_PCP+"+str(i))
    data_dfx = data_dfx.withColumn("C_PCP+"+str(i), leadCol)
    data_dfx.registerTempTable('DATA')
for column in columns:
    query = sqlContext.sql('SELECT CORR(SP_PCP,{0}) as CORRELATION FROM DATA'.format(column))
    cross_correlations.append(query.select('CORRELATION').collect()[0].CORRELATION)
#print(cross_correlations)
ac_correlations.append(cross_correlations)

# price change percent
# crypto and Nasdaq100Tech
data = data_ac.map(lambda x: (x[0],x[1],x[8],x[2]))
data_df = data.toDF(('INDEX','DATE','NT_PCP','C_PCP'))
data_df.registerTempTable('TABLE')
# we are trying to study the effect of NT_PCP change trends on C_PCP change trends
# creating time lagged values for C_PCP for upto 30 days
columns = []
columns.append('C_PCP')
cross_correlations = []
data_dfx = data_df
for i in range(1,31):
    window = Window.orderBy("Index")
    leadCol = F.lead("C_PCP", i).over(window)
    columns.append("C_PCP+"+str(i))
    data_dfx = data_dfx.withColumn("C_PCP+"+str(i), leadCol)
    data_dfx.registerTempTable('DATA')
for column in columns:
    query = sqlContext.sql('SELECT CORR(NT_PCP,{0}) as CORRELATION FROM DATA'.format(column))
    cross_correlations.append(query.select('CORRELATION').collect()[0].CORRELATION)
#print(cross_correlations)
ac_correlations.append(cross_correlations)

# tmv change percent
# crypto and nasdaq100
data = data_ac.map(lambda x: (x[0],x[1],x[5],x[3]))
data_df = data.toDF(('INDEX','DATE','N_TMVCP','C_TMVCP'))
data_df.registerTempTable('TABLE')
# we are trying to study the effect of N_TMVCP change trends on C_TMVCP change trends
# creating time lagged values for C_TMVCP for upto 30 days
columns = []
columns.append('C_TMVCP')
cross_correlations = []
data_dfx = data_df
for i in range(1,31):
    window = Window.orderBy("Index")
    leadCol = F.lead("C_TMVCP", i).over(window)
    columns.append("C_TMVCP+"+str(i))
    data_dfx = data_dfx.withColumn("C_TMVCP+"+str(i), leadCol)
    data_dfx.registerTempTable('DATA')
for column in columns:
    query = sqlContext.sql('SELECT CORR(N_TMVCP,{0}) as CORRELATION FROM DATA'.format(column))
    cross_correlations.append(query.select('CORRELATION').collect()[0].CORRELATION)
#print(cross_correlations)
ac_correlations.append(cross_correlations)

# tmv change percent
# crypto and sp100
data = data_ac.map(lambda x: (x[0],x[1],x[7],x[3]))
data_df = data.toDF(('INDEX','DATE','SP_TMVCP','C_TMVCP'))
data_df.registerTempTable('TABLE')
# we are trying to study the effect of N_TMVCP change trends on C_TMVCP change trends
# creating time lagged values for C_TMVCP for upto 30 days
columns = []
columns.append('C_TMVCP')
cross_correlations = []
data_dfx = data_df
for i in range(1,31):
    window = Window.orderBy("Index")
    leadCol = F.lead("C_TMVCP", i).over(window)
    columns.append("C_TMVCP+"+str(i))
    data_dfx = data_dfx.withColumn("C_TMVCP+"+str(i), leadCol)
    data_dfx.registerTempTable('DATA')
for column in columns:
    query = sqlContext.sql('SELECT CORR(SP_TMVCP,{0}) as CORRELATION FROM DATA'.format(column))
    cross_correlations.append(query.select('CORRELATION').collect()[0].CORRELATION)
#print(cross_correlations)
ac_correlations.append(cross_correlations)

# tmv change percent
# crypto and Nadaq100Tech
data = data_ac.map(lambda x: (x[0],x[1],x[9],x[3]))
data_df = data.toDF(('INDEX','DATE','NT_TMVCP','C_TMVCP'))
data_df.registerTempTable('TABLE')
# we are trying to study the effect of N_TMVCP change trends on C_TMVCP change trends
# creating time lagged values for C_TMVCP for upto 30 days
columns = []
columns.append('C_TMVCP')
cross_correlations = []
data_dfx = data_df
for i in range(1,31):
    window = Window.orderBy("Index")
    leadCol = F.lead("C_TMVCP", i).over(window)
    columns.append("C_TMVCP+"+str(i))
    data_dfx = data_dfx.withColumn("C_TMVCP+"+str(i), leadCol)
    data_dfx.registerTempTable('DATA')
for column in columns:
    query = sqlContext.sql('SELECT CORR(NT_TMVCP,{0}) as CORRELATION FROM DATA'.format(column))
    cross_correlations.append(query.select('CORRELATION').collect()[0].CORRELATION)
#print(cross_correlations)
ac_correlations.append(cross_correlations)

print('--BEFORE COVID--')
print('AVERAGE CROSS CORRELATIONS (0 to 30 day time lag)')
avg_bc_correlations = [sum(x)/len(x) for x in bc_correlations]
titles = ['Nadaq100 vs Crypto ', \
         'S&P100 vs Crypto', \
         'Nadaq100Tech vs Crypto' ,\
          'Nadaq100 vs Crypto ', \
         'S&P100 vs Crypto', \
         'Nadaq100Tech vs Crypto']
print('\nFor Avg Daily Price Change Percentage')
for i in [0,1,2]:
    print(titles[i]+' : '+str(avg_bc_correlations[i]))
print('\nFor Avg Daily TMV Change Percentage')
for i in [3,4,5]:
    print(titles[i]+' : '+str(avg_bc_correlations[i]))
    
print('--AFTER COVID--')
print('AVERAGE CROSS CORRELATIONS (0 to 30 day time lag)')
avg_ac_correlations = [sum(x)/len(x) for x in ac_correlations]
print('\nFor Avg Daily Price Change Percentage')
for i in [0,1,2]:
    print(titles[i]+' : '+str(avg_ac_correlations[i]))
print('\nFor Avg Daily TMV Change Percentage')
for i in [3,4,5]:
    print(titles[i]+' : '+str(avg_ac_correlations[i]))
    
# Cross correlation very low for the cases before covid 19 
# Very close to 0 in most cases

# Rise in cross correlation noted after covid
# So, yes, the drop and rise after covid in stock and crypto markets have had a mild correlation

# None of these correlation numbers are significantly close to 1 or -1 or atleast 0.5 or -0.5
# Hence, the crypto market hasn't really reflected the trading trends of the stock market
# Just both markets have been bullish or bearish almost half the time (From Analysis 2) and nothing else

# None of these correlation numbers are significantly close to 1 or -1 or atleast 0.5 or -0.5
# Hence, the crypto market hasn't really reflected the trading trends of the stock market
# Just both markets have been bullish or bearish almost half the time (From Analysis 2) and nothing else