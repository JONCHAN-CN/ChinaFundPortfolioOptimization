# -*- coding: utf-8 -*-
"""
Created on Mon Nov  5 19:02:10 2018
@author: JON7390
交易一期(基金)数据处理
"""
import logging

import numpy as np
import pandas as pd

logger = logging.getLogger('main.DataProcess')

# NAV切片数据统计
def calNAVStat(table,name):
    dateStat = table.count(axis=1)
    fundStat = table.count()
    col = list([50, 60, 70, 80, 90, 95])
    df = pd.DataFrame(np.zeros([2, 6]), columns=col, index=['dateStat', 'fundStat'])
    for i in col:
        df.loc['dateStat', i] = np.percentile(dateStat, i)
        df.loc['fundStat', i] = np.percentile(fundStat, i)
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    writer = pd.ExcelWriter('../out/2-NAV_stat_%s.xlsx'%name, engine='xlsxwriter')
    # Convert the dataframe to an XlsxWriter Excel object.
    dateStat.to_excel(writer, sheet_name='dateStat')
    fundStat.to_excel(writer, sheet_name='fundStat')
    df.to_excel(writer, sheet_name='percentileStat')
    # Close the Pandas Excel writer and output the Excel file.
    writer.save()
    return dateStat, fundStat, df


# handle term
def stripTerm(df,column,freq = 'Y'):
    df[column] = df[column].replace(np.nan, "0天")
    df[column] = df.apply(
        lambda x: (int(x[column].split('年又')[0]) + int(x[column].split('年又')[1].strip('天')) / 365.25)
        if len(x[column].split('年又')) > 1 else (int(x[column].split('年又')[0].strip('天')) / 365.25), axis=1)
    if freq == 'D':
        df[column] = df.apply(lambda x: int(x[column]*365.25),axis =1)
    else:
        pass
    return df


# cal annual return rate
def calAnnualReturn(df, return_rate, term, result):
    df[return_rate] = df.apply(lambda x: x[return_rate].strip('%').strip(), axis=1)
    df[return_rate] = df[return_rate].replace({'': 0})
    df[return_rate] = df[return_rate].replace({'-': 0})
    df[return_rate] = df[return_rate].replace({',': ''})
    df[result] = df.apply(
        lambda x: ((float(str(x[return_rate]).replace(',', '')) / 100) + 1.0) ** (1 / x[term]) - 1.0 if x[term] != 0.00000 else 0,axis=1)
    return df


# 导出表格为csv
def exportTable(tab, name = None, freq = None, index_flag=False):
    try:
        if freq is not None:
            tab.to_csv('../out/2-%s-%s.csv' % (name, freq), index=index_flag)
        else:
            tab.to_csv('../out/2-%s.csv' % name, index=index_flag)
        logger.info(str('EXPORT TABLES %s Succeed!' % name))
    except Exception as e:
        logger.exception(str('EXPORT TABLES %s Fail:%s' % (name, e)))


# 处理Manager数据
def processManager(mana_his,mana_info,mana_chg):
    # manager history
    logger.info('Processing manager history...')
    # stupid case
    name_error = [['WANG AO(汪澳)','汪澳'],['TIAN HUAN','TIAN_HUAN'],['DANIELDONGNINGSUN','孙东宁'],['IKEDA KAE','IKEDA_KAE']]
    for i in name_error:
        mana_his.loc[mana_his['manager_name'] == i[0], 'manager_name'] = i[1]
    # deal w till-now
    mana_his.loc[mana_his['end_date'] == '至今', 'end_date'] = mana_his.loc[mana_his['end_date'] == '至今', 'updated_date']
    mana_his['end_date'] = mana_his['end_date'].astype('datetime64[D]')
    # strip term of years
    mana_his = stripTerm(mana_his,'term',freq = 'Y')
    # handle return rate
    mana_his = calAnnualReturn(mana_his,return_rate = 'return_rate',term = 'term',result = 'annual_return')
    # gen weight acc to year of data
    mana_his['annual_return_weight'] = mana_his.apply(
        lambda x: 1 - (2019 - x['end_date'].year) * 0.04 if x['end_date'].year >= 2008 else 0.52, axis=1)

    # 1\ scoring on manager based on annual return rate
    mana_score = mana_his.groupby('manager_id').apply(
        lambda x: (x['annual_return'] * x['annual_return_weight'] * x['term']).sum() / x['term'].sum())  # interesting
    mana_score = pd.DataFrame(mana_score, columns=['annual_return_score'])
    # 2\ 资历（最早任职时间+最后任职时间）
    mana_score['earliest_start_date'] = mana_his.groupby(['manager_id'])['start_date'].agg('min').astype(
        'datetime64[D]')
    mana_score['latest_end_date'] = mana_his.groupby(['manager_id'])['end_date'].agg('max').astype('datetime64[D]')
    mana_score['total_term'] = mana_score.apply(lambda x: (x['latest_end_date'] - x['earliest_start_date']).days,
                                                axis=1)
    # 3\ 在行时间与比率
    mana_score['cum_on_duty_term'] = mana_his.groupby(['manager_id'])['cum_on_duty_term'].agg('max')
    mana_score = stripTerm(mana_score,'cum_on_duty_term','D') # 时间（天）
    mana_score['cum_on_duty_term_pct'] = mana_score.apply(lambda x: x['cum_on_duty_term'] / x['total_term'], axis=1) # 比率
    # 4\ manager name
    mana_score = pd.merge(mana_score, mana_his[['manager_id', 'manager_name']], how='left', left_index=True,right_on='manager_id')
    mana_score.drop_duplicates(inplace=True)

    # manager info
    logger.info('Processing manager info...')
    del mana_info['manager_name']
    mana_info = pd.merge(mana_info, mana_score, how='left', on='manager_id')
    mana_info.drop_duplicates(inplace=True)
    name = 'MANAGER_info'
    exportTable(mana_info,name = name, freq = None, index_flag=False)

    # manager changes on fund
    logger.info('Processing manager chg on funds...')
    # stupid case again
    name_error = [['WANG AO(汪澳)', '汪澳'], ['TIAN HUAN', 'TIAN_HUAN'], ['DANIELDONGNINGSUN', '孙东宁'],
                  ['IKEDA KAE', 'IKEDA_KAE'],['DANIEL DONGNING SUN', '孙东宁'],['SHI CHENG(史程)', '史程']]
    for i in name_error:
        mana_chg['fund_managers'] = mana_chg['fund_managers'].apply(lambda x: x.replace(i[0], i[1]))
    del mana_chg['created_date']
    # deal w till-now
    mana_chg.loc[mana_chg['end_date'] == '至今', 'end_date'] = mana_chg.loc[mana_chg['end_date'] == '至今', 'updated_date']
    mana_chg['end_date'] = mana_chg['end_date'].astype('datetime64[D]')
    # get latest record of each fund
    mana_chg = mana_chg.iloc[mana_chg.groupby(['fund_code'])['end_date'].agg('idxmax')]
    # gen a tall table based on managers
    mana_chg = pd.merge(mana_chg, mana_chg['fund_managers'].str.split(' ', expand=True), how='left', left_index=True,
                        right_index=True)

    p_chg = pd.DataFrame()
    for i in range(0, 3):
        exclude = list([0, 1, 2, 3])
        exclude.pop(i)
        tmp = mana_chg.loc[mana_chg[i].astype('str') != 'None', ~mana_chg.columns.isin(exclude)]
        tmp.rename(columns={i: 'single_manager'}, inplace=True)
        p_chg = pd.concat([p_chg, tmp], axis=0)
    # strip term of years
    p_chg = stripTerm(p_chg,'term')
    # gen annual return rate based on funds
    p_chg = calAnnualReturn(p_chg,'return_rate','term','annual_return_fund')
    # merge mana_his and mana_info
    tmp = mana_his[['manager_id', 'fund_code', 'manager_name']].drop_duplicates()
    p_chg = pd.merge(p_chg, tmp, how='left', left_on=['single_manager', 'fund_code'],
                     right_on=['manager_name', 'fund_code'])
    tmp = mana_info[['manager_id', 'annual_return_score', 'total_term', 'cum_on_duty_term_pct']].drop_duplicates()
    p_chg = pd.merge(p_chg, tmp, how='left', left_on=['manager_id'], right_on=['manager_id'])
    # fill nan annual_return_score with annual return rate of fund
    p_chg['annual_return_score'] = p_chg.apply(
        lambda x: x['annual_return_fund'] if str(x['annual_return_score']) == 'nan' else x['annual_return_score'],
        axis=1)
    # gen weighted annual return rate based on managers
    p_chg['weighted_annual_return_score'] = p_chg.groupby(['fund_code'])['annual_return_score'].transform('mean')

    # p_chg['total_term'] = p_chg['total_term'].apply(lambda x:str(x))
    # count_down = p_chg[p_chg['total_term'] == 'nan']
    p_chg.drop_duplicates(inplace=True)
    name = 'MANAGER_chg'
    exportTable(p_chg, name=name, freq=None, index_flag=False)
    logger.info('Done processing MANAGER!\n')
    return mana_info,p_chg


# 处理NAV数据
def processNAV(db, frequency='BM',export = False):
    logger.info('Processing NAV...')
    db['the_date'] = db['the_date'].values.astype('datetime64[D]')
    name = ''
    try:
        if db.shape[1]==9:
            name = 'CURRENCY'
            db['profit_rate'] = pd.to_numeric(db['profit_rate'].apply(lambda x: float(str(x).strip('%'))/100).fillna(0), errors='coerce')
            db['date_m'] = db['the_date'].apply(lambda x:str(x)[:7])
            db = db.groupby(['fund_code','date_m'])['profit_rate'].mean().reset_index(level = [0,1])
            db = db.pivot('date_m', 'fund_code', 'profit_rate').fillna(method='ffill')
            if 'M' in frequency:
                db = db / 12
            elif 'Q' in frequency:
                db = db / 4
            else: pass
        else:
            name = 'NAV'
            db['nav'] = pd.to_numeric(db['nav'], errors='coerce') #todo 分红如何处理？算回去？
            db = db.pivot('the_date', 'fund_code', 'nav').fillna(method='ffill').resample(frequency).asfreq()
        calNAVStat(db,name) # cal statistic
        if export:
            exportTable(db,name = name, freq = frequency, index_flag=False) # export
        logger.info(str('Done processing - [%s-%s]\n' % (name, frequency)))
        return db
    except Exception as e:
        logger.exception(str('FAIL to process - [%s-%s] \n%s\n' % (name, frequency, e)))
        return 1


def main():
    # nav & currency
    nav = pd.read_csv('../out/1-fund_nav.csv')
    nav = processNAV(nav,frequency='BM',export = False) # adjusted pct_chg
    cur = pd.read_csv('../out/1-fund_nav_currency.csv')
    cur = processNAV(cur, frequency='BM',export = False) # annualised return rate by frequency

    # manager
    mana_his = pd.read_csv('../out/1-fund_managers_his.csv', dtype=str)
    mana_info = pd.read_csv('../out/1-fund_managers_info.csv', dtype=str)
    mana_chg = pd.read_csv('../out/1-fund_managers_chg.csv', dtype=str)
    processManager(mana_his, mana_info, mana_chg)

    # TODO: fund_info

if __name__ == "__main__":
    main()