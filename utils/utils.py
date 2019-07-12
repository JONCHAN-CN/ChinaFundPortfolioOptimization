import logging

import numpy as np
import pandas as pd

from .decorator import try_exception

logger = logging.getLogger('main.export')


@try_exception()
def calNAVStat(table, name):
    '''NAV数据切片统计'''
    # gen stat
    dateStat = table.count(axis=1)
    fundStat = table.count()
    col = list([50, 60, 70, 80, 90, 95])
    df = pd.DataFrame(np.zeros([2, 6]), columns=col, index=['dateStat', 'fundStat'])
    for i in col:
        df.loc['dateStat', i] = np.percentile(dateStat, i)
        df.loc['fundStat', i] = np.percentile(fundStat, i)

    # Create a Pandas Excel writer using XlsxWriter as the engine.
    writer = pd.ExcelWriter('../out/2-NAV_stat_%s.xlsx' % name, engine='xlsxwriter')

    # Convert the dataframe to an XlsxWriter Excel object.
    dateStat.to_excel(writer, sheet_name='dateStat')
    fundStat.to_excel(writer, sheet_name='fundStat')
    df.to_excel(writer, sheet_name='percentileStat')

    # Close the Pandas Excel writer and output the Excel file.
    writer.save()

    return dateStat, fundStat, df


# 导出表格为csv
@try_exception()
def exportTable(tab, name=None, freq=None, index_flag=False):
    if freq is not None:
        tab.to_csv('../out/2-%s-%s.csv' % (name, freq), index=index_flag)
    else:
        tab.to_csv('../out/2-%s.csv' % name, index=index_flag)
    logger.info(str('\nEXPORT TABLES %s Succeed!' % name))


def stripTerm(df, column, freq='Y'):
    '''handle term -> x 年又 X 天'''
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
    '''cal annual return rate'''
    df[return_rate] = df.apply(lambda x: x[return_rate].strip('%').strip(), axis=1)
    df[return_rate] = df[return_rate].replace(['', '-'], 0).replace(',', '')
    # df[return_rate] = df[return_rate].replace({'-': 0})
    # df[return_rate] = df[return_rate].replace({',': ''})
    df[result] = df.apply(
        lambda x: ((float(str(x[return_rate]).replace(',', '')) / 100) + 1.0) ** (1 / x[term]) - 1.0 if x[
                                                                                                            term] != 0.00000 else 0,
        axis=1)
    return df


def replaceManaName(df, name_col):
    # stupid case
    name_error = [['WANG AO(汪澳)', '汪澳'], ['TIAN HUAN', 'TIAN_HUAN'], ['DANIELDONGNINGSUN', '孙东宁'],
                  ['IKEDA KAE', 'IKEDA_KAE'], ['DANIEL DONGNING SUN', '孙东宁'], ['SHI CHENG(史程)', '史程']]
    for i in name_error:
        df[name_col] = df[name_col].apply(lambda x: x.replace(i[0], i[1]))
    return df
