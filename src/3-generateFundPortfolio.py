# -*- coding: utf-8 -*-
"""
Created on Sun Nov 25 16:16:10 2018

@author: JON7390
交易一期(基金)模型
"""
# %matplotlib inline
import numpy as np
import matplotlib.pyplot as plt
import cvxopt as opt
from cvxopt import blas, solvers
import pandas as pd
import itertools
import logging
from scipy.special import comb
import time
import datetime

logging.basicConfig(level=logging.INFO,
                    filename='../log/3-genFundPortfolio.log',
                    filemode='a',
                    datefmt='%Y/%m/%d %H:%M:%S',
                    format='%(levelname)s %(asctime)s %(funcName)s %(lineno)d %(message)s'
                    )
logger = logging.getLogger(__name__)
chlr = logging.StreamHandler() # 输出到控制台的handler
logger.addHandler(chlr)


### Harry Markowitz ###########################################################
# precise corvariance matrix
def preciseCorvariance(dbTable):
    start = time.time()
    corTotal = comb(dbTable.shape[1],2)
    count = 0
    corMat = pd.DataFrame(np.zeros(shape = [dbTable.shape[1],dbTable.shape[1]]),columns = dbTable.columns,index = dbTable.columns)
    for a, b in itertools.combinations(dbTable.columns, 2):
        preCor = dbTable[[a,b]].fillna(method = 'ffill')
        # preCor = preCor.fillna(method = 'ffill')
        preCor = preCor[preCor.count(axis =1)==2]
        preCor = preCor.T
        corMat.loc[a,b] = np.cov(preCor)[1,0]
        corMat.loc[b,a] = corMat.loc[a,b]
        count=count+1
        if count%5000 ==0:
            print("Calculating Covariance-[%d/ %f%%]..."%(count,count*100/corTotal))
    print("Calculating Variance..." )
    for c in itertools.combinations(dbTable.columns, 1):
        preCor = dbTable[c[0]].fillna(method = 'ffill').dropna()
        corMat.loc[c,c] = np.var(preCor.T)
    end = time.time()
    msg = "Time used : "+str(end - start)
    logger.info(msg)
    return corMat

def fitHM(dataTables,frequency='BQ-DEC',year = 7):
    today = datetime.date.today()
    goback = today - datetime.timedelta(days=year*365.25)
    dataTable = dataTables.set_index('the_date')
    dataTable.index = dataTable.index.astype('datetime64[D]')
    dataTable = dataTable.fillna(method='bfill').resample(frequency).asfreq()
    returns = dataTable.pct_change()
    returns = returns.loc[goback:]
    return returns

# optimizer
def optimalPortfolio(returns, nbr_assets):
    n = nbr_assets
    returns = returns.iloc[:, :nbr_assets]
    returns = returns.T
    returns = np.asmatrix(returns.values)

    N = 100 # scalar?
    mus = [10**(5.0 * t/N - 1.0) for t in range(N)] # what is this?
    
    # Convert to cvxopt matricess    
    S = opt.matrix(np.cov(returns)) # S-> covariance matrix  inhere   
    pbar = opt.matrix(np.mean(returns, axis=1)) # pbar -> expected returns
    # Create constraint matrices    
    G = -opt.matrix(np.eye(n))   # negative n x n identity matrix
    h = opt.matrix(0.0, (n ,1)) # all weight >= 0    
    A = opt.matrix(1.0, (1, n))
    b = opt.matrix(1.0) # weights sum up to 1    
    # Calculate [efficient frontier] weights using quadratic programming    
    portfolios = [solvers.qp(mu*S, -pbar, G, h, A, b)['x']  for mu in mus]  # x-> weighted portfolios| solvers.qp(P,q,G,h,A,b)
    
    # CALCULATE RISKS AND RETURNS FOR FRONTIER
    returns = [blas.dot(pbar, x) for x in portfolios]
    risks = [np.sqrt(blas.dot(x, S*x)) for x in portfolios]
    # CALCULATE THE 2ND DEGREE POLYNOMIAL OF THE FRONTIER CURVE    
    m1 = np.polyfit(returns, risks, 2) 
    x1 = np.sqrt(m1[2] / m1[0]) # TODO: what is this?
    # CALCULATE THE [OPTIMAL PORTFOLIO]
    wt = solvers.qp(opt.matrix(x1 * S), -pbar, G, h, A, b)['x']
    return np.asarray(wt), returns, risks

def harryMarkowitz(nbr_assets,navReturns):
    # Turn off progress printing
    solvers.options['show_progress'] = False
    # Cal each portfolio
    comTotal = comb(navReturns.shape[1], nbr_assets)
    count = 0
    port = pd.DataFrame()
    # port = pd.DataFrame(np.zeros(shape = [1,nbr_assets*2]))
    start = time.time()
    for col in itertools.combinations(navReturns.columns, nbr_assets):
        try:
            navReturn = navReturns[list(col)]
            weights, returns, risks = optimalPortfolio(navReturn, nbr_assets)
            fund = pd.DataFrame(list(col)).T
            po = pd.DataFrame(data=weights.T)
            port.append(pd.DataFrame(pd.concat([fund,po],axis = 1)),ignore_index=True)
            print(navReturn.columns, weights, returns, risks)
        except Exception as e:
            msg = str("\n本组合计算失败：%s\n" % str(col))
            # logger.exception(msg)
        count = count+1
        if count%100 ==0:
            print("Calculating Portfolio-[%d/ %f%%]..."%(count,count*100/comTotal))
    end = time.time()
    msg = "Time used : " + str(end - start)
    logger.info(msg)
    return port

    
### Main ######################################################################
def main():  
    nav = pd.read_csv('../out/2-fund_nav-nav-B.csv')
    navReturns = fitHM(nav,'BQ-DEC', 8) # Table, Resample Frequency, Years to go
    port = harryMarkowitz(5,navReturns)
    port.to_csv('../out/3-port.csv')
    
if __name__ == "__main__":
    main()