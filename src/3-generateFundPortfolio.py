# -*- coding: utf-8 -*-
"""
Created on Sun Nov 25 16:16:10 2018

@author: JON7390
交易一期(基金)模型
"""
import itertools
import logging
import time

import cvxopt as opt
import numpy as np
import pandas as pd
from cvxopt import blas, solvers
from dateutil.relativedelta import relativedelta
from scipy.special import comb

from src.utils import DataProcess

logging.basicConfig(level=logging.INFO,
                    filename='../log/3-genFundPortfolio.log',
                    filemode='a',
                    datefmt='%Y/%m/%d %H:%M:%S',
                    format='%(levelname)s %(asctime)s %(funcName)s %(lineno)d %(message)s'
                    )
logger = logging.getLogger('main')
logger.addHandler(logging.StreamHandler())


def sharpeRatio(df,optional_risk,single = False):
    perYear = 1
    if 'BQ' in fit_frequency:
        perYear = 4
    elif 'BM' in fit_frequency:
        perYear = 12
    risk_free_rate = (1 + risk_free) ** (1 / perYear) - 1
    if single == False:
        df['sharpeRatio'] = df.apply(lambda x:(x['returns']-risk_free_rate)/x['risks'],axis =1)
    elif single == True:
        df = (df-risk_free_rate)/optional_risk
    return df


def preciseCorvariance(nav): # calculate precise corvariance
    start = time.time()
    df = nav.copy()
    # del df['the_date']
    df = df.fillna(method='ffill').pct_change()
    corMat = pd.DataFrame(np.zeros(shape = [df.shape[1],df.shape[1]]),columns = df.columns,index = df.columns)
    corTotal = comb(df.shape[1],2)
    verbose = 100
    count = 0
    # each iteration
    for a, b in itertools.combinations(df.columns, 2):
        preCor = df[[a,b]]
        preCor = preCor[preCor.count(axis =1)==2].T.astype('float')
        # preCor = preCor.T.astype('float')
        tmp = np.cov(preCor)[1,0]
        corMat.loc[a,b] = tmp
        corMat.loc[b,a] = tmp
        count = count+1
        if count%verbose ==0:
           logger.info("\nCalculating Covariance-[%d/ %f%%]..."%(count,count*verbose/corTotal))
    logger.info("\nCalculating Variance...")
    for c in itertools.combinations(df.columns, 1):
        preCor = df[c[0]].dropna()
        corMat.loc[c,c] = np.var(preCor.T)
    end = time.time()
    msg = "\nTime used : "+str(end - start)+"\n\n"
    logger.info(msg)
    return corMat


def fitHM(nav,frequency='BQ-DEC',ytg = 8):
    goback = nav.index[-1] - relativedelta(years = ytg)
    df = nav.copy()
    df = df.fillna(method='bfill').resample(frequency).asfreq().pct_change()
    # df = df.fillna(method='ffill').fillna(method='bfill').resample(frequency).asfreq()
    # roi = df.pct_change()
    # mode = 'train'
    roi = df.loc[goback:]
    return roi


def optimalPortfolio(navReturn, nbr, pre = None): # harry markoviz optimizer
    # Convert to cvxopt matricess
    navReturn = np.asmatrix(navReturn.T.values)
    if isinstance(pre,pd.DataFrame):
        S = opt.matrix(np.asarray(pre))
    else:
        S = opt.matrix(np.cov(navReturn))  # S-> covariance matrix
    pbar = opt.matrix(np.mean(navReturn, axis=1)) # pbar -> expected returns

    # Create constraint matrices
    G = -opt.matrix(np.eye(nbr))   # negative nbr x nbr identity matrix
    h = opt.matrix(0.0, (nbr ,1)) # all weight >= 0
    A = opt.matrix(1.0, (1, nbr))
    b = opt.matrix(1.0) # weights sum up to 1

    #  scale desired returns
    N = 100
    mus = [10**(4 * t/N - 2) for t in range(N)] # desired portfolio returns

    # Calculate [efficient frontier] weights using quadratic programming    
    portfolios = [solvers.qp(mu*S, -pbar, G, h, A, b)['x'] for mu in mus]  # x-> weighted portfolios| solvers.qp(P,q,G,h,A,b)

    # CALCULATE RISKS AND RETURNS FOR FRONTIER
    returns = [blas.dot(pbar, x) for x in portfolios]
    risks = [np.sqrt(blas.dot(x, S*x)) for x in portfolios]
    r_r = pd.DataFrame({'risks':risks,'returns':returns})

    # FIT THE 2ND DEGREE POLYNOMIAL OF THE FRONTIER CURVE
    m1 = np.polyfit(returns, risks, 2) # polyfit: x,y,degree

    # CALCULATE 3 OUTPUT:  MAXIMUM SHARPE RATIO/ DEFAULT/ MINIMUM RISK
    srp = sharpeRatio(r_r,None,False)
    srp_y = float(srp.loc[srp['sharpeRatio'] == srp['sharpeRatio'].max(), 'returns'])
    # srp_y = srp[0] if isinstance(srp_y,seri) else srp_y # TODO AVOID MULTIPLE RESULT
    y1 = np.sqrt(m1[2] / m1[0])
    min_y = -m1[1]/(2*m1[0]) # -(b/2a)
    port =[]
    for i in [srp_y,y1,min_y]:
        op_wt = solvers.qp(opt.matrix(i * S), -pbar, G, h, A, b)['x']  # CALCULATE THE [OPTIMAL PORTFOLIO WEIGHT] with min risk
        op_return = blas.dot(pbar, op_wt)  # return under OPTIMAL PORTFOLIO
        op_risk = np.sqrt(blas.dot(op_wt, S * op_wt))  # risk under OPTIMAL PORTFOLIO
        port.append( [np.asarray(op_wt),op_return,op_risk])
    return port


def harryMarkowitz(nbr, return_vec, preCor = None):
    start = time.time()
    solvers.options['show_progress'] = False # Turn off progress printing
    # Cal each portfolio
    comTotal = comb(return_vec.shape[1], nbr) # TODO ADD TDQM
    count = 0
    verbose = 100
    po = pd.DataFrame()
    for col in itertools.combinations(return_vec.columns, nbr):
        try:
            # slice data for each iteration
            navReturn = return_vec[list(col)]
            pre = None
            if isinstance(preCor,pd.DataFrame):
                pre = preCor.loc[list(col),list(col)]
            # optimal solver
            port = optimalPortfolio(navReturn, nbr, pre)
            # deal w/ result
            fund = pd.DataFrame(list(col)).T.add_prefix('fundCode_') # fund_code
            srp_wt = pd.DataFrame(data=port[0][0].T).add_prefix('srp_portfolio_')
            def_wt = pd.DataFrame(data=port[1][0].T).add_prefix('def_portfolio_')
            min_wt = pd.DataFrame(data=port[2][0].T).add_prefix('min_portfolio_')
            port_ = pd.DataFrame(pd.concat(
                [fund, srp_wt, pd.Series(port[0][1]), pd.Series(port[0][2]), def_wt, pd.Series(port[1][1]),
                 pd.Series(port[1][2]), min_wt, pd.Series(port[2][1]), pd.Series(port[2][2])], axis=1))
            port_.rename(columns={0: 'srp_return', 1: 'srp_risk',2: 'def_return', 3: 'def_risk',4: 'min_return', 5: 'min_risk'},inplace=True)
            po = pd.concat([po,port_],axis =0,ignore_index = True)
        except Exception as e:
            logger.exception(str("\n本组合计算失败：%s" % str(col)))
        # verbose
        count = count+1
        if count%verbose == 0:
            logger.info("\n\nCalculating Portfolio-[%d/ %f%%]..."%(count,count*verbose/comTotal))
    end = time.time()
    logger.info("\nTime used : " + str(end - start))
    return po


def filterManager(mana_info,mana_chg, annual_return_score = 0.075, cum_on_duty_term_pct = 0.55, annual_return_fund= 0.075, term = 1.0, weighted_annual_return_score =0.075, mode ='loose'):
    mana_chg['manager_id'].replace({np.nan: 0}, inplace=True)

    # filterManager based on manager
    f1 = mana_info.loc[(mana_info['annual_return_score'] >= annual_return_score),['manager_name','manager_id']]  # filterManager 1
    f2 = mana_info.loc[(mana_info['cum_on_duty_term_pct'] >= cum_on_duty_term_pct), ['manager_name','manager_id']]  # filterManager 2
    # filterManager 1
    mana_chg['f1'] = mana_chg.apply(lambda x: 1 if (str(x['manager_id']) in list(f1['manager_id'])) else 0,axis =1)
    mana_chg.loc[mana_chg['manager_id']==0,'f1'] = mana_chg[mana_chg['manager_id']==0].apply(lambda x: 1 if (x['single_manager'] in list(f1['manager_name'])) else 0,axis =1)
    # filterManager 2
    mana_chg['f2'] = mana_chg.apply(lambda x: 1 if (str(x['manager_id']) in list(f2['manager_id'])) else 0,axis =1)
    mana_chg.loc[mana_chg['manager_id']==0,'f2'] = mana_chg[mana_chg['manager_id']==0].apply(lambda x: 1 if (x['single_manager'] in list(f2['manager_name'])) else 0,axis =1)

    # filterManager based on fund
    # filterManager 3
    mana_chg['f3'] = mana_chg.apply(lambda x: 1 if float(x['annual_return_fund']) >= annual_return_fund else 0,axis =1)
    # filterManager 4
    mana_chg['f4'] = mana_chg.apply(lambda x: 1 if float(x['term'])>= term else 0,axis =1)
    # filterManager 5
    mana_chg['f5'] = mana_chg.apply(lambda x: 1 if float(x['weighted_annual_return_score'])>= weighted_annual_return_score else 0,axis =1)

    # check
    for i in list(['f1','f2','f3','f4','f5']):
        logger.info('After filterManager %s:'%i + str(pd.Series(mana_chg.loc[mana_chg[i]>=1,'fund_code'].unique()).count()))
    fund_list =pd.DataFrame()
    if mode =='loose':
        fund_list = mana_chg.loc[((mana_chg['f1']+mana_chg['f2']+mana_chg['f3']+mana_chg['f4']>=4) | (mana_chg['f2']+mana_chg['f3']+mana_chg['f4']+mana_chg['f5']>=4)),:]
    elif mode =='strict':
        fund_list = mana_chg.loc[(mana_chg['f1'] + mana_chg['f2'] + mana_chg['f3'] + mana_chg['f4'] + mana_chg['f5']>= 5), :]
    else:
        return 1
    logger.info('After ALL FILTER:'+str(len(fund_list['fund_code'].unique())))
    return fund_list['fund_code']


def main():
    global end_date,risk_free,fit_frequency
    risk_free = 0.035
    fit_frequency = 'BM' # 'BQ-DEC'
    validation_period = 3
    portfolio_nbr=[3]

    # load data
    nav = pd.read_csv('../out/1-fund_nav.csv',dtype={'fund_code':str})
    cur = pd.read_csv('../out/1-fund_nav_currency.csv',dtype={'fund_code':str})
    mana_his = pd.read_csv('../out/1-fund_managers_his.csv', dtype=str)
    mana_info = pd.read_csv('../out/1-fund_managers_info.csv', dtype=str)
    mana_chg = pd.read_csv('../out/1-fund_managers_chg.csv', dtype=str)

    # data process
    nav = DataProcess.processNAV(nav,fit_frequency) # adjusted prices
    cur = DataProcess.processNAV(cur, fit_frequency) # annualised return rate by frequency TODO MERGE IT
    mana_info,p_chg = DataProcess.processManager(mana_his,mana_info,mana_chg) # manager

    # filter data
    fund_list = filterManager(mana_info,p_chg,annual_return_score=0.07, cum_on_duty_term_pct=0.60, annual_return_fund=0.07,
                              term=1.5, weighted_annual_return_score=0.075, mode='strict') # fund list after filtering managers
    # tmp = ['000029', '000064', '000149'] # good
    # tmp = ['000127', '002624', '519732'] # bad
    nav_train = nav[list(fund_list)]

    # split data
    nav_valid = nav_train.iloc[-(validation_period+1):,:] # split data for validation
    nav_train = nav_train.iloc[:-(validation_period),:] # split data for modeling

    # training
    preCor = preciseCorvariance(nav_train) # gen precise corvariance
    return_vec = fitHM(nav_train, fit_frequency,10)  # gen return of interest: data, resample Frequency, years to go back
    for nbr in portfolio_nbr:
        port = harryMarkowitz(nbr,return_vec,preCor) #TODO MULTITHREAD
        port.round(3).to_csv('../out/2-portfolio_%d.csv'%nbr,index = False)

    # validation
    nav_valid = nav_valid.iloc[:-1,:] # unnecessary next batch
    nav_valid = (1+nav_valid.fillna(method='bfill').resample(fit_frequency).asfreq().pct_change()).cumprod().iloc[-1,:]-1
    for nbr in portfolio_nbr:
        res_valid = pd.DataFrame()
        port = pd.read_csv('../out/3-portfolio_%d.csv'%nbr,dtype=dict(('fundCode_'+str(i),'str') for i in range(nbr))).round(3)
        for i in range(len(port)):
            slice =port.iloc[i,:]
            fundCode = slice[:nbr]
            val = nav_valid[list(fundCode)]
            for idx in [((nbr+2)*i+nbr) for i in range(nbr)]:
                wt = slice[idx:idx+nbr]
                slice = slice.append(pd.Series(np.dot(wt, val)))
            res_valid = pd.concat([res_valid, slice.to_frame().T], axis=0)
        res_valid.round(3).to_csv('../out/3-validation_%d.csv' % nbr, index=False)

if __name__ == "__main__":
    main()