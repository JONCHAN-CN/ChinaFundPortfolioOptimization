import logging
import sys
from datetime import datetime as dt

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from pandas.tseries.offsets import BDay

from .decorator import try_exception

logger = logging.getLogger('main.export')


@try_exception
def calNAVStat(table, name):
    """NAV数据切片统计"""
    # gen stat
    dateStat = table.count(axis=1)
    fundStat = table.count()
    col = list([50, 60, 70, 80, 90, 95])
    df = pd.DataFrame(np.zeros([2, 6]), columns=col, index=['dateStat', 'fundStat'])
    for i in col:
        df.loc['dateStat', i] = np.percentile(dateStat, i)
        df.loc['fundStat', i] = np.percentile(fundStat, i)

    # Create a Pandas Excel writer using XlsxWriter as the engine.
    cur_date = str(dt.now().strftime('%Y%m%d'))
    writer = pd.ExcelWriter('./data/2-STAT_%s_%s.xlsx' % (name, cur_date), engine='xlsxwriter')

    # Convert the dataframe to an XlsxWriter Excel object.
    dateStat.to_excel(writer, sheet_name='dateStat')
    fundStat.to_excel(writer, sheet_name='fundStat')
    df.to_excel(writer, sheet_name='percentileStat')

    # Close the Pandas Excel writer and output the Excel file.
    writer.save()

    return dateStat, fundStat, df


@try_exception
def exportDataframe(tab, name=None, freq=None, index_flag=False):
    """导出表格为csv"""
    cur_date = str(dt.now().strftime('%Y%m%d'))
    if freq is not None:
        tab.to_csv('./data/2-%s-%s-%s.csv' % (name, freq, cur_date), index=index_flag)
    else:
        tab.to_csv('./data/2-%s-%s.csv' % (name, cur_date), index=index_flag)
    logger.info(str('\nEXPORT TABLES %s Succeed!' % name))


def exportQuery(mySQL, tab):
    try:
        pd.DataFrame(mySQL.selectDistinct(tab)).to_csv('./data/1-%s.csv' % tab, index=False)
        logger.info(str('EXPORT TABLES %s Succeed!' % tab))
    except Exception as e:
        logger.exception(str('EXPORT TABLES %s Fail:\n%s' % (tab, e)))


def stripTerm(df, column, freq='Y'):
    """handle term -> x 年又 X 天"""
    df[column] = df[column].replace(np.nan, "0天")
    df[column] = df.apply(
        lambda x: (int(x[column].split('年又')[0]) + int(x[column].split('年又')[1].strip('天')) / 365)
        if len(x[column].split('年又')) > 1 else (int(x[column].split('年又')[0].strip('天')) / 365), axis=1)
    if freq == 'D':
        df[column] = df.apply(lambda x: int(x[column] * 365), axis=1)
    else:
        pass
    return df


def calAnnualReturn(df, return_rate, term, result):
    """cal annual return rate"""
    df[return_rate] = df.apply(lambda x: x[return_rate].strip('%').strip(), axis=1)
    df[return_rate] = df[return_rate].replace(['', '-'], 0).replace(',', '')
    # df[return_rate] = df[return_rate].replace({'-': 0})
    # df[return_rate] = df[return_rate].replace({',': ''})
    df[result] = df.apply(
        lambda x: ((float(str(x[return_rate]).replace(',', '')) / 100) + 1.0) ** (1 / x[term]) - 1.0 \
            if x[term] != 0.00000 else 0, axis=1)
    return df


def replaceManaName(df, name_col):
    # stupid case
    name_error = [['WANG AO(汪澳)', '汪澳'], ['TIAN HUAN', 'TIAN_HUAN'], ['DANIELDONGNINGSUN', '孙东宁'],
                  ['IKEDA KAE', 'IKEDA_KAE'], ['DANIEL DONGNING SUN', '孙东宁'], ['SHI CHENG(史程)', '史程']]
    for i in name_error:
        df[name_col] = df[name_col].apply(lambda x: x.replace(i[0], i[1]))
    return df


def select_date(engine):
    # customize backtest date
    input_date = input('CUSTOMIZE DATE(YYYY-MM-DD):')
    if len(input_date) != 10:
        backtest_date = dt.now()
    else:
        backtest_date = dt.strptime(input_date, '%Y-%m-%d')

    # retrive latest available nav date(a.within latest 10 days; b.enough fund nav data(>=90%);)
    _sql = 'select date,count(*) from %s group by date order by date desc limit 10'
    date_list = pd.read_sql_query(_sql % 'nav', engine)
    latest_nav_date = 0
    for i in range(len(date_list)):
        if date_list.iloc[i]['count(*)'] >= date_list['count(*)'].max() * 0.9:
            latest_nav_date = dt.strptime(date_list.iloc[i]['date'], '%Y-%m-%d')
    if latest_nav_date == 0:
        logger.exception('NO ENOUGH DATA TO GO ON')
        sys.exit()

    # get pratical backtest date(a.weekdays;)
    backtest_date = min(backtest_date, latest_nav_date)
    if backtest_date.weekday() < 5:
        pass
    else:
        backtest_date = backtest_date - BDay(1)

    logger.info('SELECTED DATE - %s' % str(backtest_date))
    return backtest_date


def fitHM(nav, frequency='BQ-DEC', years=None):
    df = nav.copy()
    df = df.fillna(method='bfill').resample(frequency).asfreq().pct_change()
    if years is not None:
        starting = nav.index[-1] - relativedelta(years=years)
        df = df.loc[starting:]

    return df


def listAllFiles(rootdir):
    import os
    _files = []
    list = os.listdir(rootdir)  # 列出文件夹下所有的目录与文件
    for i in range(0, len(list)):
        path = os.path.join(rootdir, list[i])
        if os.path.isdir(path):
            _files.extend(listAllFiles(path))
        if os.path.isfile(path):
            _files.append(path)
    return _files
