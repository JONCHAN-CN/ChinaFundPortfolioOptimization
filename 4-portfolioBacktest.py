# -*- coding: utf-8 -*-
"""
Created on Sun Nov 25 16:16:10 2018

@author: JON7390
交易一期(基金)回测
"""
import sys
import traceback
from datetime import datetime as dt

import pandas as pd
import yaml
from sqlalchemy import create_engine

from utils import PyMySQL, logger
from utils.utils import select_date

logger = logger.init_logger()


def load_data(engine, bk_date):
    logger.info('loading data')

    # portfolio
    _sql = 'select distinct * from fund_portfolio_3 where expire_date is null'
    portfolio = pd.read_sql_query(_sql, con=engine, index_col='id', parse_dates=['train_date', 'gen_date'])
    portfolio['expire'] = 0

    # model date
    tr_date = tuple(portfolio['train_date'].astype('str').unique())

    # nav
    _sql = ('select * from %s where date in %s or date = \'%s\'' % ('nav', tr_date, str(bk_date)[:10])).replace(
        ',)', ')')
    nav = pd.read_sql_query(_sql, engine)
    nav['date'] = nav['date'].astype('datetime64[D]')

    return portfolio, nav


def merge_data(portfolio, nav, bk_date):
    logger.info('merging data')

    # merge train-date nav into portfolio
    nav = nav[['date', 'fund_code', 'nav', 'add_nav']].set_index(['date', 'fund_code'])
    for i in range(3):
        portfolio = pd.merge(portfolio, nav, left_on=['train_date', 'fundCode_%d' % i], right_index=True, how='left')
        portfolio[['nav', 'add_nav']] = portfolio[['nav', 'add_nav']].fillna(0).astype('float')
        portfolio.loc[portfolio['nav'] == 0.0, 'expire'] = 1
        portfolio.rename(columns={'nav': 'nav_%d' % i, 'add_nav': 'add_nav_%d' % i}, inplace=True)

    # merge newest nav into portfolio
    portfolio['backtest_date'] = bk_date
    portfolio['backtest_date'] = portfolio['backtest_date'].astype('datetime64[D]')
    nav.drop('nav', axis=1, inplace=True, errors='ignore')
    for i in range(3):
        portfolio = pd.merge(portfolio, nav, left_on=['backtest_date', 'fundCode_%d' % i], right_index=True, how='left')
        portfolio['add_nav'] = portfolio['add_nav'].fillna(0).astype('float')
        portfolio.rename(columns={'add_nav': 'last_add_nav_%d' % i}, inplace=True)

    # split portfolio to be expired
    expire_port = portfolio[portfolio['expire'] == 1]
    portfolio = portfolio[portfolio['expire'] != 1]

    # period & expected return
    portfolio['period'] = portfolio['backtest_date'] - portfolio['train_date']
    portfolio['period'] = portfolio.apply(lambda x: x['period'].days, axis=1)
    portfolio = portfolio[portfolio['period'] > 0]
    if len(portfolio):
        portfolio['exp_return'] = portfolio.apply(lambda x: x['period'] / 30 * x['returns'], axis=1)
    else:
        logger.exception('No portfolio generated before backtest date')
        sys.exit()

    return expire_port, portfolio


def backtest_data(portfolio):
    logger.info('calculating data')

    # growth of return
    portfolio['act_return'] = 0.0
    cal_each_return = lambda x: (x['last_add_nav_%d' % i] / x['add_nav_%d' % i] - 1) * x['portfolio_%d' % i]
    for i in range(3):
        portfolio['act_return'] = portfolio['act_return'] + portfolio.apply(cal_each_return, axis=1)

    return portfolio


def expire_portfolio(portfolio, expire_zero, config, mySQL):
    # judge if any portfolio should be expired
    portfolio['act_return_per'] = portfolio['act_return'] / portfolio['period'] * 30
    threshold = portfolio.act_return_per.quantile(config['Backtest']['pct'])
    portfolio['top_act'] = portfolio.apply(
        lambda x: 1 if (x['act_return_per'] > threshold and x['act_return'] > 0) else 0, axis=1)
    portfolio['achieve_exp'] = portfolio.apply(
        lambda x: 1 if (x['act_return'] >= x['exp_return'] * config['Backtest']['coe'] and x['act_return'] > 0) else 0,
        axis=1)
    portfolio['expire'] = portfolio.apply(lambda x: 1 if x['top_act'] + x['achieve_exp'] == 0 else 0, axis=1)

    # write back expire info
    expire_id = portfolio[portfolio['expire'] == 1].index
    expire_date = str(dt.now().strftime('%Y-%m-%d'))
    if len(expire_id) > 1:
        expire_id = str(tuple(set(expire_id).union(set(expire_zero.index))))
        expire_date = str(dt.now().strftime('%Y-%m-%d'))
        exp_sql = 'UPDATE fund_portfolio_3 SET expire_date= \'%s\' WHERE id IN %s;' % (expire_date, expire_id)
        expire_cnt = mySQL.sql(exp_sql)
    elif len(expire_id) == 1:
        exp_sql = 'UPDATE fund_portfolio_3 SET expire_date= \'%s\' WHERE id = %s;' % (expire_date, expire_id[0])
        expire_cnt = mySQL.sql(exp_sql)
    else:
        expire_cnt = 0

    logger.info('%d portfolios has been expired' % expire_cnt)


def export_backtest(portfolio, engine, port_cnt):
    # export to backtest table
    backtest_col = ['id', 'exp_return', 'act_return', 'period', 'backtest_date', 'train_date', 'act_return_per']
    backtest = portfolio[portfolio['expire'] == 0].reset_index()[backtest_col]
    if len(backtest) > 0:
        backtest.to_sql('fund_backtest_%d' % port_cnt, con=engine, if_exists='replace', index=False)
        logger.info('%d backtest results has been saved' % len(backtest))
    return backtest


def plot_10(col, type, engine):
    import matplotlib.pyplot as plt
    import seaborn as sns
    sns.set()

    if type == 'history':
        # plot historical top 10 of portfolio
        _sql = 'select * from fund_backtest_3 where id in(select * from (select id from fund_backtest_3 order by %s desc limit 10) as t)'
    else:
        # plot most recent top 10 of portfolio
        _sql = 'select * from fund_backtest_3 where id in (select * from (select id from fund_backtest_3 where backtest_date in (select max(backtest_date) from fund_backtest_3 ) order by %s desc limit 10) as t)'

    _10 = pd.read_sql_query(_sql % col, con=engine)
    _10 = _10[_10['act_return_per'].abs() <= 0.8]

    # lp = sns.pointplot(x="backtest_date", y=col, hue="id", data=_10)
    # lp = sns.lineplot(x="backtest_date", y=col, hue="id", style="id", markers=True, data=_10)
    lp = sns.lineplot(x="backtest_date", y=col, hue="id", data=_10)

    plt.show()


def params_perf(portfolio, engine):
    # cal accuracy of the model by date
    gp_key = ['batchid', 'backtest_date']
    params_pct = portfolio[portfolio['expire'] == 0].groupby(gp_key)[['exp_return', 'act_return']].mean().add_prefix(
        'avg_')
    params_pct['pct'] = params_pct['avg_act_return'] / params_pct['avg_exp_return']
    params_pct['pct'] = params_pct['pct'].astype("str")
    try:
        params_pct.reset_index().to_sql('params_benchmark', con=engine, if_exists='replace', index=False)
    except Exception:
        logger.error(f'FAIL TO INSERT INTO PARAMS_BENCHMARK\n{traceback.format_exc()}')


def welcome():
    logger.info('\nPlz input no. to execute commands:\n'
                '1.Backtest PORTFOLIO\n'
                '2.Plot TOP 10 PORTFOLIO by actual cumulative returns\n'
                '3.Plot TOP 10 PORTFOLIO by actual monthly returns\n'
                '4.Find Most weighted fund\n'
                '5.Count effective PORTFOLIO\n'
                '0.Exit\n'
                '\n'
                'Command NO.: ')
    n = sys.stdin.readline().strip('\n')
    if int(n) in [0, 1, 2, 3, 4, 5]:
        return int(n)
    else:
        logger.info('INVALID INPUT\n')
        welcome()


def main():
    port_cnt = 3  # TODO loop over port of 2/3/4
    config = yaml.load(open('config.yaml'))
    db_con = config['MySQL']
    engine = create_engine(
        "mysql://%s:%s@%s/%s?charset=utf8mb4" % (db_con['user'], db_con['passwd'], db_con['host'], db_con['db']))
    mySQL = PyMySQL.PyMySQL()

    while True:
        n = welcome()
        if n == 1:
            # backtest date
            bk_date = select_date(engine)

            # load portfolio & nav
            portfolio, nav = load_data(engine, bk_date)

            # merge portfolio, nav & bk_date
            expire_zero, portfolio = merge_data(portfolio, nav, bk_date)

            # backtest
            portfolio = backtest_data(portfolio)

            # expire portfolio
            mySQL._init_(**db_con)
            expire_portfolio(portfolio, expire_zero, config, mySQL)
            mySQL.dispose()

            # export backtest
            _ = export_backtest(portfolio, engine, port_cnt)

            # cal accuracy of the model by date
            params_perf(portfolio, engine)  # TODO VIZ

        elif n == 2:
            # plot top 10 portfolio by actual cumulative return
            plot_10('act_return', 'history', engine)
            plot_10('act_return', 'recent', engine)

        elif n == 3:
            # plot top 10 portfolio by actual monthly return
            plot_10('act_return_per', 'history', engine)
            plot_10('act_return_per', 'recent', engine)

        elif n == 4:
            _sql = 'select * from fund_portfolio_%d where id in (select * from (select id from fund_backtest_%d where backtest_date in (select max(backtest_date) from fund_backtest_%d ) order by act_return_per desc limit 100) as t)' % (
                port_cnt, port_cnt, port_cnt)
            _100 = pd.read_sql_query(_sql, con=engine)
            tall_port = pd.DataFrame()
            for i in range(port_cnt):
                tall_port = pd.concat([tall_port, _100[['fundCode_%d' % i, 'portfolio_%d' % i]].rename(
                    {'fundCode_%d' % i: 'fundCode', 'portfolio_%d' % i: 'portfolio'}, axis=1)], axis=0)
            piv_port = tall_port.groupby('fundCode').sum().rename({'portfolio': 'total weight'}, axis=1)
            piv_port['avg weight'] = tall_port.groupby('fundCode').mean()
            piv_port.sort_values(by='avg weight', ascending=False, inplace=True)

            logger.info('Most weighted fund: \n%s' % piv_port)

        elif n == 5:
            _sql = 'select COUNT(*) as cnt from (SELECT distinct ID FROM fund.fund_portfolio_%d where expire_date is null) as t' % port_cnt
            cnt = pd.read_sql_query(_sql, con=engine)['cnt'][0]
            logger.info('%d effective portfolio' % cnt)


if __name__ == "__main__":
    main()
