# -*- coding: utf-8 -*-
"""
Created on Sun Nov 25 16:16:10 2018

@author: JON7390
交易一期(基金)模型
"""
import datetime
import itertools
import pickle as pk
from multiprocessing import Lock, Process, Queue, cpu_count

import cvxopt as opt
import numpy as np
import pandas as pd
import yaml
from cvxopt import blas, solvers
from dateutil.relativedelta import relativedelta
from pandas.tseries.offsets import BDay
from scipy.special import comb
from sqlalchemy import create_engine

from utils import dataProcess, logger
from utils.decorator import *

solvers.options['show_progress'] = False  # Turn off progress printing
logger = logger.init_logger()


def select_train_date(engine):
    # customize train date
    input_date = input('TRAIN DATE(YYYY-MM-DD):')
    if len(input_date) != 10:
        train_date = dt.now()
    else:
        train_date = dt.strptime(input_date, '%Y-%m-%d')

    # adjust train date to end of the month
    train_date = train_date.replace(day=28) + datetime.timedelta(days=4)
    train_date = train_date - datetime.timedelta(days=train_date.day)

    # retrive latest available nav date
    # _sql = 'select date,count(*) from %s group by date order by date desc limit 10'
    # date_list = pd.read_sql_query(_sql % 'fund_nav', engine)
    # latest_nav_date = 0
    # for i in range(len(date_list)):
    #     if date_list.iloc[i]['count(*)'] >= date_list['count(*)'].max() * 0.9:
    #         latest_nav_date = dt.strptime(date_list.iloc[i]['date'], '%Y-%m-%d')
    # if latest_nav_date == 0:
    #     logger.exception('NO ENOUGH DATA TO GO ON')
    #     sys.exit()

    # get pratical train date
    # train_date = min(train_date, latest_nav_date)
    if train_date.weekday() < 5:
        pass
    else:
        train_date = train_date - BDay(1)

    logger.info('train date - %s' % str(train_date))
    return train_date.strftime('%Y-%m-%d')


def sharpeRatio(df, optional_risk, risk_free, fit_frequency, single=False):
    perYear = 1
    if 'BQ' in fit_frequency:
        perYear = 4
    elif 'BM' in fit_frequency:
        perYear = 12
    risk_free_rate = (1 + risk_free) ** (1 / perYear) - 1
    if single == False:
        df['sharpeRatio'] = df.apply(lambda x: (x['returns'] - risk_free_rate) / x['risks'], axis=1)
    elif single == True:
        df = (df - risk_free_rate) / optional_risk
    return df


@time_elapsed
def preciseCorvariance(nav, years):
    '''calculate precise corvariance'''
    starting = nav.index[-1] - relativedelta(years=years)
    df = nav.copy()
    # del df['date']
    df = df.fillna(method='ffill').pct_change()  # nav only
    df = df.loc[starting:]
    corMat = pd.DataFrame(np.zeros(shape=[df.shape[1], df.shape[1]]), columns=df.columns, index=df.columns)
    corTotal = comb(df.shape[1], 2)
    verbose = 1000
    count = 0

    # covariance
    for a, b in itertools.combinations(df.columns, 2):
        preCor = df[[a, b]]
        preCor = preCor[preCor.count(axis=1) == 2].T.astype('float')
        tmp = np.cov(preCor)[1, 0]
        corMat.loc[a, b] = tmp
        corMat.loc[b, a] = tmp
        count = count + 1
        if count % verbose == 0:
            logger.info("Calculating Covariance-[%d/ %f%%]..." % (count, count * 100 / corTotal))

    # variance
    logger.info("\nCalculating Variance...")
    for c in itertools.combinations(df.columns, 1):
        preCor = df[c[0]].dropna()
        corMat.loc[c, c] = np.var(preCor.T)

    return corMat


def fitHM(nav, frequency='BQ-DEC', years=10):
    starting = nav.index[-1] - relativedelta(years=years)
    df = nav.copy()
    df = df.fillna(method='bfill').resample(frequency).asfreq().pct_change()
    roi = df.loc[starting:]
    return roi


def optimalPortfolio(navReturn, nbr, pre, risk_free, fit_frequency):  # harry markoviz optimizer
    # Convert to cvxopt matricess
    navReturn = np.asmatrix(navReturn.T.values)
    if isinstance(pre, pd.DataFrame):
        S = opt.matrix(np.asarray(pre))
    else:
        S = opt.matrix(np.cov(navReturn))  # S-> covariance matrix
    pbar = opt.matrix(np.mean(navReturn, axis=1))  # pbar -> expected returns

    # Create constraint matrices
    G = -opt.matrix(np.eye(nbr))  # negative nbr x nbr identity matrix
    h = opt.matrix(0.0, (nbr, 1))  # all weight >= 0
    A = opt.matrix(1.0, (1, nbr))
    b = opt.matrix(1.0)  # weights sum up to 1

    #  scale desired returns
    N = 100
    mus = [10 ** (4 * t / N - 2) for t in range(N)]  # desired portfolio returns

    # Calculate [efficient frontier] weights using quadratic programming
    portfolios = [solvers.qp(mu * S, -pbar, G, h, A, b)['x'] for mu in
                  mus]  # x-> weighted portfolios| solvers.qp(P,q,G,h,A,b)

    # CALCULATE RISKS AND RETURNS FOR FRONTIER
    returns = [blas.dot(pbar, x) for x in portfolios]
    risks = [np.sqrt(blas.dot(x, S * x)) for x in portfolios]
    r_r = pd.DataFrame({'risks': risks, 'returns': returns})

    # FIT THE 2ND DEGREE POLYNOMIAL OF THE FRONTIER CURVE
    m1 = np.polyfit(returns, risks, 2)  # polyfit: x,y,degree

    # CALCULATE 3 OUTPUT:  MAXIMUM SHARPE RATIO/ DEFAULT/ MINIMUM RISK
    srp = sharpeRatio(r_r, None, risk_free, fit_frequency, False)
    srp_y = float(srp.loc[srp['sharpeRatio'] == srp['sharpeRatio'].max(), 'returns'])
    # srp_y = srp[0] if isinstance(srp_y,seri) else srp_y # TODO AVOID MULTIPLE RESULT
    min_y = -m1[1] / (2 * m1[0])  # -(b/2a)
    if (m1[2] / m1[0]) < 0:
        y1 = min_y
    else:
        y1 = np.sqrt(m1[2] / m1[0])
    port = []
    for i in [srp_y, y1, min_y]:
        op_wt = solvers.qp(opt.matrix(i * S), -pbar, G, h, A, b)[
            'x']  # CALCULATE THE [OPTIMAL PORTFOLIO WEIGHT] with min risk
        op_return = blas.dot(pbar, op_wt)  # return under OPTIMAL PORTFOLIO
        op_risk = np.sqrt(blas.dot(op_wt, S * op_wt))  # risk under OPTIMAL PORTFOLIO
        port.append([np.asarray(op_wt), op_return, op_risk])
    return port


@time_elapsed
def calculating_proc(nbr, return_vec, preCor, in_queue, out_queue, lock_in, lock_out, risk_free, fit_frequency):
    ''''multiprocess -> input queue + function + output queue'''
    in_count = 0
    out_count = 0
    while in_queue.qsize():
        lock_in.acquire()
        col = in_queue.get()
        in_count = in_count + 1
        lock_in.release()
        epo_return = return_vec[list(col)]
        epo_cor = None
        if isinstance(preCor, pd.DataFrame):
            epo_cor = preCor.loc[list(col), list(col)]
        # train and output
        try:
            port = optimalPortfolio(epo_return, nbr, epo_cor, risk_free, fit_frequency)  # optimal solver
            lock_out.acquire()
            out_queue.put([col, port])
            lock_out.release()
            out_count = out_count + 1
        except Exception as e:
            logger.exception(str("\n本组合计算失败：%s" % str(col)))
    logger.info('[%s] in-%d | out-%d - exit calculating process' % (str(os.getpid()), in_count, out_count))


def formating_proc(nbr, train_date, out_queue, risk_free, fit_frequency):
    config = yaml.load(open('./dep/config.yaml'))
    mySQL = config['MySQL']
    engine = create_engine("mysql://%s:%s@%s/%s" % (mySQL['user'], mySQL['passwd'], mySQL['host'], mySQL['db']))
    po = pd.DataFrame()
    for_count = 0
    awake = 0

    # format single portfolio from queue
    while True:
        if out_queue.empty():
            awake = awake + 1
            logger.info('%d time(s) to halt formatting process' % awake)
            import time
            time.sleep(15)
            if awake == 5:
                logger.info('[%s] format-%d - exit formatting process' % (str(os.getpid()), for_count))
                break
        else:
            awake = 0
            for_count = for_count + 1
            col_port = out_queue.get()
            col = col_port[0]
            port = col_port[1]
            port_ = pd.DataFrame()

            # deal w/ result
            ## fund_code
            fund = pd.DataFrame(list(col)).T.add_prefix('fundCode_')
            ## weight, type, others
            for idx, label in ([0, 'srp'], [1, 'def'], [2, 'min']):
                wt = pd.DataFrame(data=port[idx][0].T).add_prefix('portfolio_')
                port_ = pd.concat([port_, pd.DataFrame(pd.concat(
                    [fund, wt, pd.Series(port[idx][1]), pd.Series(port[idx][2]), pd.Series(label),
                     pd.Series(str(train_date)[:10])], axis=1))], axis=0, ignore_index=True)
            ## concat fund code & portfolio
            po = pd.concat([po, port_], axis=0, ignore_index=True)
            ## verbose
            if for_count % 1000 == 0:
                logger.info('formatting portfolio %d' % for_count)

    logger.info('exporting portfolio')
    po.rename(columns={0: 'returns', 1: 'risks', 2: 'label', 3: 'train_date'}, inplace=True)

    # calculate sharpe ratio
    po = sharpeRatio(po, None, risk_free, fit_frequency, False)

    # drop duplicate entries
    po = po.round(3)
    po['unique_flag'] = po.apply(lambda x: int(x[0]) * x[3] + int(x[1]) * x[4] + int(x[2]) * x[5], axis=1)
    po = po.iloc[po['unique_flag'].drop_duplicates().index, :-1]
    logger.info('%d portfolio generated' % po.shape[1])

    # export params
    param = pd.DataFrame({**config['Filter'], **config['Portfolio']}, index=[0])
    param['batchid'] = str(dt.now().strftime('%Y%m%d%H'))
    pk.dump(param, open('./data/param_%s.dat' % train_date, 'wb'), True)
    # param = pk.load(open('./data/param_%s.dat' % train_date, 'rb'))
    # param.to_sql(name='params', con=engine, if_exists='append', index=False)

    # export portfolio to disk & database
    po['batchid'] = str(dt.now().strftime('%Y%m%d%H'))
    po.to_csv('./data/3-portfolio_%d_%s.csv' % (nbr, str(dt.now().strftime('%Y%m%d'))), index=False)
    pk.dump(po, open('./data/po_%s.dat' % train_date, 'wb'), True)
    # po = pk.load(open('./data/po_%s.dat' % train_date, 'rb'))
    # po.to_sql(name='fund_portfolio_3', con=engine, if_exists='append', index=False)


@time_elapsed
def harryMarkowitz(nbr, return_vec, preCor=None):
    global risk_free, fit_frequency, lock_in, lock_out, engine
    in_queue = Queue()  # multiprocess -> input queue
    out_queue = Queue()  # multiprocess -> output queue
    proc_list = []

    logger.info('\n%d CPU on board, starting %d process' % (cpu_count(), proc_nbr))

    # gen input queue
    for col in itertools.combinations(return_vec.columns, nbr):
        in_queue.put(col)
    logger.info('%d combination in queue' % in_queue.qsize())

    # calculate portfolio & export
    ## create processes
    for w in range(proc_nbr - 1):
        p = Process(target=calculating_proc,
                    args=(nbr, return_vec, preCor, in_queue, out_queue, lock_in, lock_out, risk_free, fit_frequency))
        proc_list.append(p)
    proc_list.append(
        Process(target=formating_proc, args=(nbr, return_vec.index[-1], out_queue, risk_free, fit_frequency)))
    ## starting process
    [p.start() for p in proc_list]
    ## completing process
    [p.join() for p in proc_list]


def filterManager(mana_info, mana_chg, annual_return_score=0.075, cum_on_duty_term_pct=0.55, annual_return_fund=0.075,
                  term=1.0, weighted_annual_return_score=0.075, mode='loose'):
    mana_chg['manager_id'].replace({np.nan: 0}, inplace=True)

    # generate filter of manager based on manager
    return_score = mana_info[mana_info['annual_return_score'] >= annual_return_score][['manager_name', 'manager_id']]
    term_pct = mana_info[mana_info['cum_on_duty_term_pct'] >= cum_on_duty_term_pct][['manager_name', 'manager_id']]

    # apply filter of manager based on manager
    mana_chg['return_score'] = mana_chg.apply(
        lambda x: 1 if (str(x['manager_id']) in list(return_score['manager_id'])) else 0, axis=1)
    # mana_chg.loc[mana_chg['manager_id'] == 0, 'return_score'] = mana_chg[mana_chg['manager_id'] == 0].apply(
    #     lambda x: 1 if (x['single_manager'] in list(return_score['manager_name'])) else 0, axis=1)
    mana_chg['term_pct'] = mana_chg.apply(lambda x: 1 if (str(x['manager_id']) in list(term_pct['manager_id'])) else 0,
                                          axis=1)
    # mana_chg.loc[mana_chg['manager_id'] == 0, 'term_pct'] = mana_chg[mana_chg['manager_id'] == 0].apply(
    #     lambda x: 1 if (x['single_manager'] in list(term_pct['manager_name'])) else 0, axis=1)

    # apply filter of manager based on fund
    mana_chg['return_rate'] = mana_chg.apply(lambda x: 1 if float(x['annual_return_fund']) >= annual_return_fund else 0,
                                             axis=1)
    mana_chg['term'] = mana_chg.apply(lambda x: 1 if float(x['term']) >= term else 0, axis=1)
    mana_chg['weighted'] = mana_chg.apply(
        lambda x: 1 if float(x['weighted_annual_return_score']) >= weighted_annual_return_score else 0, axis=1)

    # log filter application
    for i in list(['return_score', 'term_pct', 'return_rate', 'term', 'weighted']):
        cnt = pd.Series(mana_chg.loc[mana_chg[i] >= 1, 'fund_code'].unique()).count()
        logger.info('After filterManager %s - %s:' % (i, str(cnt)))

    # generate fund list
    fund_list = pd.DataFrame()
    if mode == 'loose':
        fund_list = mana_chg[(mana_chg[['return_score', 'term_pct', 'return_rate', 'term']].sum(axis=1) >= 4) | (
                mana_chg[['term_pct', 'return_rate', 'term', 'weighted']].sum(axis=1) >= 4)]
    elif mode == 'strict':
        fund_list = mana_chg.loc[
            mana_chg[['return_score', 'term_pct', 'return_rate', 'term', 'weighted']].sum(axis=1) >= 5]
    else:
        return 1

    logger.info('After ALL FILTER:' + str(len(fund_list['fund_code'].unique())))
    return fund_list['fund_code']


def main():
    config = yaml.load(open('./dep/config.yaml'))

    global risk_free, fit_frequency, portfolio_nbr, proc_nbr, lock_in, lock_out
    [risk_free, fit_frequency, portfolio_nbr, ytg] = [*config['Portfolio'].values()]
    proc_nbr = int(input('Number of process: '))
    lock_in = Lock()
    lock_out = Lock()

    # local params
    db_con = config['MySQL']
    engine = create_engine(
        "mysql://%s:%s@%s/%s?charset=utf8mb4" % (db_con['user'], db_con['passwd'], db_con['host'], db_con['db']))
    filter = config['Filter']
    nav_file = config['NAVFile']
    mana_file = [*config['ManagerFile'].values()]

    # train date
    train_date = select_train_date(engine)

    # data
    if os.path.exists(nav_file['res_path'] % train_date):
        nav_model = pk.load(open(nav_file['res_path'] % train_date, 'rb'))
    else:
        # load nav
        logger.info('loading data')
        _sql = 'select distinct * from %s where date <=\'%s\''
        nav = pd.read_sql_query(_sql % ('fund_nav', train_date), engine)
        # cur = pd.read_sql_query(_sql % ('fund_nav_currency', train_date), engine)

        # process nav
        nav = dataProcess.processNAV(nav, fit_frequency, True)  # adjusted prices
        # cur = dataProcess.processCUR(cur, fit_frequency, True)  # annualised return rate by frequency TODO MERGE IT

        # process manager
        mana_info, p_chg = dataProcess.processManager(*mana_file)

        # manager filter list
        fund_list = filterManager(mana_info, p_chg, **filter)  # fund list after filtering managers
        nav_model = nav[list(fund_list)]
        nav_model = nav_model.T.drop_duplicates().T

        # dump
        pk.dump(nav_model, open(nav_file['res_path'] % train_date, 'wb'), True)

    # gen precise corvariance
    preCor = preciseCorvariance(nav_model, ytg)

    # gen return of interest
    return_vec = fitHM(nav_model, fit_frequency, ytg)  # data, resample Frequency, years to go back

    # gen portfolio
    [harryMarkowitz(nbr, return_vec, preCor) for nbr in portfolio_nbr]

    logger.info('ALL DONE')


if __name__ == "__main__":
    main()
