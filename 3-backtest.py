# -*- coding: utf-8 -*-
"""
Created on Sun Nov 25 16:16:10 2018

@author: JON7390
交易一期(基金)回测
"""
import sys
from datetime import datetime as dt

import pandas as pd
import seaborn as sns
import yaml
from pandas.tseries.offsets import BDay
from sqlalchemy import create_engine

from utils import PyMySQL, logger

logger = logger.init_logger()


def select_backtest_date(engine):
    # customize backtest date
    input_date = input('BACKTEST DATE(YYYY-MM-DD):')
    if len(input_date) != 10:
        backtest_date = dt.now()
    else:
        backtest_date = dt.strptime(input_date, '%Y-%m-%d')

    # retrive latest available nav date
    _sql = 'select date,count(*) from %s group by date order by date desc limit 10'
    date_list = pd.read_sql_query(_sql % 'fund_nav', engine)
    latest_nav_date = 0
    for i in range(len(date_list)):
        if date_list.iloc[i]['count(*)'] >= date_list['count(*)'].max() * 0.9:
            latest_nav_date = dt.strptime(date_list.iloc[i]['date'], '%Y-%m-%d')
    if latest_nav_date == 0:
        logger.exception('NO ENOUGH DATA TO GO ON')
        sys.exit()

    # get pratical backtest date
    backtest_date = min(backtest_date, latest_nav_date)
    if backtest_date.weekday() < 5:
        pass
    else:
        backtest_date = backtest_date - BDay(1)

    logger.info('backtest date - %s' % str(backtest_date))
    return backtest_date


def load_data(engine, bk_date):
    logger.info('loading data')

    # portfolio
    _sql = 'select distinct * from fund_portfolio_3 where expire_date is null'
    portfolio = pd.read_sql_query(_sql, con=engine, index_col='id', parse_dates=['train_date', 'gen_date'])
    portfolio['expire'] = 0

    # model date
    tr_date = tuple(portfolio['train_date'].astype('str').unique())

    # nav
    _sql = ('select * from %s where date in %s or date = \'%s\'' % ('fund_nav', tr_date, str(bk_date)[:10])).replace(
        ',)', ')')
    nav = pd.read_sql_query(_sql, engine)
    nav['date'] = nav['date'].astype('datetime64[D]')

    # TODO CUR_NAV
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
    cal_each_return = lambda x: ((x['last_add_nav_%d' % i] - x['add_nav_%d' % i]) / x['nav_%d' % i]) * x[
        'portfolio_%d' % i]
    for i in range(3):
        portfolio['act_return'] = portfolio['act_return'] + portfolio.apply(cal_each_return, axis=1)

    return portfolio


def expire_portfolio(portfolio, expire_zero, config, mySQL):
    # judge if any portfolio should be expired
    threshold = portfolio.act_return.quantile(config['Backtest']['pct'])
    portfolio['top_act'] = portfolio.apply(lambda x: 1 if x['act_return'] > threshold else 0, axis=1)
    portfolio['achieve_exp'] = portfolio.apply(
        lambda x: 1 if (x['act_return'] >= x['exp_return'] * config['Backtest']['coe'] and x['exp_return'] > 0) else 0,
        axis=1)
    portfolio['expire'] = portfolio.apply(lambda x: 1 if x['top_act'] + x['achieve_exp'] == 0 else 0, axis=1)

    # write back expire info
    expire_id = str(tuple(set(portfolio[portfolio['expire'] == 1].index).union(set(expire_zero.index))))
    expire_date = str(dt.now().strftime('%Y-%m-%d'))
    exp_sql = 'UPDATE fund_portfolio_3 SET expire_date= \'%s\' WHERE id IN %s;' % (expire_date, expire_id)
    mySQL.sql(exp_sql)


def export_backtest(portfolio, engine):
    # export to backtest table
    backtest_col = ['id', 'exp_return', 'act_return', 'period', 'backtest_date', 'train_date']
    backtest = portfolio[portfolio['expire'] == 0].reset_index()[backtest_col]
    # backtest = backtest[backtest['train_date'] < backtest['backtest_date']]
    if len(backtest) > 0:
        backtest.to_sql('fund_backtest_3', con=engine, if_exists='append', index=False)
    return backtest


def plot_10(type, engine):
    if type == 'history':
        # plot historical top 10 of portfolio
        _sql = 'select * from fund_backtest_3 where id in(select * from (select id from fund_backtest_3 order by act_return desc limit 10) as t)'

    else:
        # plot most recent top 10 of portfolio
        _sql = 'select * from fund_backtest_3 where id in (select * from (select id from fund_backtest_3 where backtest_date in (select max(backtest_date) from fund_backtest_3 ) order by act_return desc limit 10) as t)'

    _10 = pd.read_sql_query(_sql, con=engine)
    _10 = _10[_10['act_return'].abs() <= 0.5]
    sns.pointplot(x="backtest_date", y="act_return", hue="id", data=_10)

    logger.info('%s top 10 - %s' % (type, _10.id.unique()))


def params_perf(portfolio, engine):
    # cal accuracy of the model by date
    gp_key = ['batchid', 'backtest_date']
    params_pct = portfolio[portfolio['expire'] == 0].groupby(gp_key)[['exp_return', 'act_return']].mean().add_prefix(
        'avg_')
    params_pct['pct'] = params_pct['avg_act_return'] / params_pct['avg_exp_return']
    params_pct.reset_index().to_sql('params_benchmark', con=engine, if_exists='replace', index=False)


def main():
    config = yaml.load(open('./dep/config.yaml'))
    db_con = config['MySQL']
    engine = create_engine(
        "mysql://%s:%s@%s/%s?charset=utf8mb4" % (db_con['user'], db_con['passwd'], db_con['host'], db_con['db']))
    mySQL = PyMySQL.PyMySQL()

    # backtest date
    bk_date = select_backtest_date(engine)

    # TODO loop over 3/4/5

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
    backtest = export_backtest(portfolio, engine)

    # cal accuracy of the model by date
    params_perf(portfolio, engine)

    # plot top 10 portfolio
    plot_10('history', engine)
    plot_10('recent', engine)


if __name__ == "__main__":
    main()
