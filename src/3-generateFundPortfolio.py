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
    print("Calculating Variance..."        
    for c in itertools.combinations(dbTable.columns, 1):
        preCor = dbTable[c[0]].fillna(method = 'ffill').dropna()
        corMat.loc[c,c] = np.var(preCor.T)
    end = time.time()
    msg = "Time used : "+str(end - start)
    logger.info(msg)
    return corMat
# optimizer
def optimalPortfolio(returns,nbr_assets):
    n = nbr_assets
    returns = np.asmatrix(returns.T)
	# scalar?
    N = 100
    mus = [10**(5.0 * t/N - 1.0) for t in range(N)] # TODO: what is this?
    
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

def harryMarkowitz(nbr_assets,dataTable):
	# Turn off progress printing 
    solvers.options['show_progress'] = False
    
    dataTable.set_index('the_date',inplace=True)
    return_vec = dataTable.pct_change()	
    # for dev
    return_vec = return_vec.iloc[:,:nbr_assets]    
    weights, returns, risks = optimalPortfolio(return_vec,nbr_assets)
    print(return_vec.columns,weights, returns, risks)
    
### Main ######################################################################
def main():  
    nav = pd.read_csv('../out/2.1-fund_nav-nav-B.csv')
    # navRe = resampleData(nav,'M') # Table, Frequency
    # navStat = calStat(navRe)
    harryMarkowitz(4,nav)
    
if __name__ == "__main__":
    main()  