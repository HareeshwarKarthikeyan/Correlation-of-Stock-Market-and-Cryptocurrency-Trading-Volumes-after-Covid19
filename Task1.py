# Analysis question 1
# How do the overall comparisons of price and volume changes across the markets look like?

from pyspark import SparkContext
from pyspark.sql import SQLContext
sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)
data = sqlContext.read.csv('data.csv',header=False)
data.show(5)
# Order of columns
# 'DATE', 'C_PCP', 'C_TMVCP', 'N_PCP', 'N_TMVCP', 'SP_PCP', 'SP_TMVCP', 'NT_PCP', 'NT_TMVCP'

data = data.rdd.map(lambda x: (x[0],float(x[1]),float(x[2]),float(x[3]),float(x[4]),float(x[5]),float(x[6]),float(x[7]),float(x[8])) )

# Scatter plots
import matplotlib.pyplot as plt 
# c-pcp vs n_pcp
cpcp = data.map(lambda x: x[1]).collect()
npcp = data.map(lambda x: x[3]).collect()
fig1 = plt.figure(1, figsize=(7, 7))
plt.xlabel('Price change percentages in Crypto Index')
plt.ylabel('Price change percentages in Nasdaq100 Index')
plt.title('c-pcp vs n-pcp')
plt.scatter(cpcp,npcp)
fig1.savefig('c-pcp vs n-pcp.png', dpi=600) 
# c-pcp vs sp_pcp
plt.clf()
cpcp = data.map(lambda x: x[1]).collect()
sppcp = data.map(lambda x: x[5]).collect()
fig2 = plt.figure(1, figsize=(7, 7))
plt.xlabel('Price change percentages in Crypto Index')
plt.ylabel('Price change percentages in S&P100 Index')
plt.title('c-pcp vs sp-pcp')
plt.scatter(cpcp,sppcp)
fig2.savefig('c-pcp vs sp-pcp.png', dpi=600) 
# c-pcp vs nt_pcp
plt.clf()
cpcp = data.map(lambda x: x[1]).collect()
ntpcp = data.map(lambda x: x[7]).collect()
fig3 = plt.figure(1, figsize=(7, 7))
plt.xlabel('Price change percentages in Crypto Index')
plt.ylabel('Price change percentages in Nasdaq100Tech Index')
plt.title('c-pcp vs nt-pcp')
plt.scatter(cpcp,ntpcp)
fig3.savefig('c-pcp vs nt-pcp.png', dpi=600) 

# Scatter plots
# c-tmvcp vs n_tmvcp
plt.clf()
ctmv = data.map(lambda x: x[2]).collect()
ntmv = data.map(lambda x: x[4]).collect()
fig1 = plt.figure(1, figsize=(7, 7))
plt.xlabel('TMV change percentages in Crypto Index')
plt.ylabel('TMV change percentages in Nasdaq100 Index')
plt.title('c-tmvcp vs n-tmvcp')
plt.scatter(ctmv,ntmv)
fig1.savefig('c-tmvcp vs n-tmvcp.png', dpi=600)
# c-tmvcp vs sp_tmvcp
plt.clf()
ctmv = data.map(lambda x: x[2]).collect()
sptmv = data.map(lambda x: x[6]).collect()
fig2 = plt.figure(1, figsize=(7, 7))
plt.xlabel('TMV change percentages in Crypto Index')
plt.ylabel('TMV change percentages in S&P100 Index')
plt.title('c-tmvcp vs sp-tmvcp')
plt.scatter(ctmv,sptmv)
fig2.savefig('c-tmvcp vs sp-tmvcp.png', dpi=600)
# c-tmvcp vs nt_tmvcp
plt.clf()
ctmv = data.map(lambda x: x[2]).collect()
nttmv = data.map(lambda x: x[8]).collect()
fig3 = plt.figure(1, figsize=(7, 7))
plt.xlabel('TMV change percentages in Crypto Index')
plt.ylabel('TMV change percentages in Nasdaq100Tech Index')
plt.title('c-tmvcp vs nt-tmvcp')
plt.scatter(ctmv,nttmv)
fig3.savefig('c-tmvcp vs nt-tmvcp.png', dpi=600)

# The plots display the overall distribution of the changes in crypto index against each stock index considered 

data_df = data.toDF(('DATE', 'C_PCP', 'C_TMVCP', 'N_PCP', 'N_TMVCP', 'SP_PCP', 'SP_TMVCP', 'NT_PCP', 'NT_TMVCP'))

# Simple correlation metric
data_df.registerTempTable('data')
# c-pcp vs n_pcp
print('Correlation between daily average price change percentages between the crypto index and nasdaq100')
cor = sqlContext.sql('SELECT CORR(C_PCP,N_PCP) AS CORRELATION FROM DATA')
cor.show()
# c-pcp vs sp_pcp
print('Correlation between daily average price change percentages between the crypto index and sp100')
cor = sqlContext.sql('SELECT CORR(C_PCP,SP_PCP) AS CORRELATION FROM DATA')
cor.show()
# c-pcp vs nt_pcp
print('Correlation between daily average price change percentages between the crypto index and nasdaqTech100')
cor = sqlContext.sql('SELECT CORR(C_PCP,NT_PCP) AS CORRELATION FROM DATA')
cor.show()

# Simple correlation metric
# c-tmvcp vs n_tmvcp
print('Correlation between daily average traded monetary volume change percentages between the crypto index and nasdaq100')
cor = sqlContext.sql('SELECT CORR(C_TMVCP,N_TMVCP) AS CORRELATION FROM DATA')
cor.show()
# c-tmvcp vs sp_tmvcp
print('Correlation between daily average traded monetary volume change percentages between the crypto index and sp100')
cor = sqlContext.sql('SELECT CORR(C_TMVCP,SP_TMVCP) AS CORRELATION FROM DATA')
cor.show()
# c-tmvcp vs nt_tmvcp
print('Correlation between daily average traded monetary volume change percentages between the crypto index and nasdaqTech100')
cor = sqlContext.sql('SELECT CORR(C_TMVCP,NT_TMVCP) AS CORRELATION FROM DATA')
cor.show()

# COMMENT
# small positive correlation for the price changes
# almost nothing for the tmv
# this correlation is doesn't take into account the series nature of the data
#       and doesn't reveal any temporal correlation for the time series data
# but from simply observing this numerically, crypto shows a small positive correlation in the price changes with the stocks
#          although not significant
# but the traded volume changes show almost zero correlation which could be an indicator that the money flow into 
#     these two markets could be very much independent
# We shall look into the correlation in more detail in the third analysis task

# VISUALISATION PLOT
# getting weekly data points
dates = data_df.rdd.map(lambda x: x[0]).collect()[0::7]
c_pcp = data_df.rdd.map(lambda x: x[1]).collect()[0::7]
n_pcp = data_df.rdd.map(lambda x: x[3]).collect()[0::7]
sp_pcp = data_df.rdd.map(lambda x: x[5]).collect()[0::7]
nt_pcp = data_df.rdd.map(lambda x: x[7]).collect()[0::7]
c_tmvcp = data_df.rdd.map(lambda x: x[2]).collect()[0::7]
n_tmvcp = data_df.rdd.map(lambda x: x[4]).collect()[0::7]
sp_tmvcp = data_df.rdd.map(lambda x: x[6]).collect()[0::7]
nt_tmvcp = data_df.rdd.map(lambda x: x[8]).collect()[0::7]


plt.rcParams['figure.figsize'] = [15, 10]
plt.clf()
plt.figure(1, figsize=(15, 10))
plt.plot_date(dates, n_pcp,color='blue',linestyle='-',marker = 'None',label='Nasdaq100')
plt.plot_date(dates, c_pcp,color='red',linestyle='-',marker = 'None',label='Crypto')
plt.xticks(dates[0::9])
plt.title('Prices')
plt.xlabel('Date')
plt.ylabel('Change percentage')
plt.axvline(x=(len(dates)/2),label='Covid Onset')
plt.legend()
fig1 = plt.gcf()
fig1.savefig('c-pcp vs n-pcp_lines.png', dpi=600) 
# plt.show()

plt.rcParams['figure.figsize'] = [15, 10]
plt.clf()
plt.plot_date(dates, sp_pcp,color='blue',linestyle='-',marker = 'None',label='S&P100')
plt.plot_date(dates, c_pcp,color='red',linestyle='-',marker = 'None',label='Crypto')
plt.xticks(dates[0::9])
plt.title('Prices')
plt.xlabel('Date')
plt.ylabel('Change percentage')
plt.axvline(x=(len(dates)/2),label='Covid Onset')
plt.legend()
fig1 = plt.gcf()
fig1.savefig('c-pcp vs sp-pcp_lines.png', dpi=600)
# plt.show()

plt.rcParams['figure.figsize'] = [15, 10]
plt.clf()
plt.plot_date(dates, nt_pcp,color='blue',linestyle='-',marker = 'None',label='Nasdaq100Tech')
plt.plot_date(dates, c_pcp,color='red',linestyle='-',marker = 'None',label='Crypto')
plt.xticks(dates[0::9])
plt.title('Prices')
plt.xlabel('Date')
plt.ylabel('Change percentage')
plt.axvline(x=(len(dates)/2),label='Covid Onset')
plt.legend()
fig1 = plt.gcf()
fig1.savefig('c-pcp vs nt-pcp_lines.png', dpi=600)
# plt.show()

plt.rcParams['figure.figsize'] = [15, 10]
plt.clf()
plt.plot_date(dates, n_tmvcp,color='blue',linestyle='-',marker = 'None',label='Nasdaq100')
plt.plot_date(dates, c_tmvcp,color='red',linestyle='-',marker = 'None',label='Crypto')
plt.xticks(dates[0::9])
plt.title('Traded Monetary Volume')
plt.xlabel('Date')
plt.ylabel('Change percentage')
plt.axvline(x=(len(dates)/2),label='Covid Onset')
plt.legend()
fig1 = plt.gcf()
fig1.savefig('c-tmvcp vs n-tmvcp_lines.png', dpi=600)
# plt.show()

plt.rcParams['figure.figsize'] = [15, 10]
plt.clf()
plt.plot_date(dates, sp_tmvcp,color='blue',linestyle='-',marker = 'None',label='S&P100')
plt.plot_date(dates, c_tmvcp,color='red',linestyle='-',marker = 'None',label='Crypto')
plt.xticks(dates[0::9])
plt.title('Traded Monetary Volume')
plt.xlabel('Date')
plt.ylabel('Change percentage')
plt.axvline(x=(len(dates)/2),label='Covid Onset')
plt.legend()
fig1 = plt.gcf()
fig1.savefig('c-tmvcp vs sp-tmvcp_lines.png', dpi=600)
# plt.show()

plt.rcParams['figure.figsize'] = [15, 10]
plt.clf()
plt.plot_date(dates, nt_tmvcp,color='blue',linestyle='-',marker = 'None',label='Nasdaq100Tech')
plt.plot_date(dates, c_tmvcp,color='red',linestyle='-',marker = 'None',label='Crypto')
plt.xticks(dates[0::9])
plt.title('Traded Monetary Volume')
plt.xlabel('Date')
plt.ylabel('Change percentage')
plt.axvline(x=(len(dates)/2),label='Covid Onset')
plt.legend()
fig1 = plt.gcf()
fig1.savefig('c-tmvcp vs nt-tmvcp_lines.png', dpi=600)
# plt.show()
