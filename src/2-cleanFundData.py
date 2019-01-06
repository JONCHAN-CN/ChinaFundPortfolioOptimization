# -*- coding: utf-8 -*-
"""
Created on Mon Nov  5 19:02:10 2018
@author: JON7390
交易一期(基金)数据处理
"""
import logging
import time

import numpy as np
import pandas as pd

# import sys; print('Python %s on %s' % (sys.version, sys.platform))
# sys.path.extend(['C:\\Users\\JON7390\\OneDrive\\文档\\Individual_Project\\Fund_Profolio', 'C:/Users/JON7390/OneDrive/文档/Individual_Project/Fund_Profolio'])


logging.basicConfig(level=logging.INFO,
                    filename='../log/2-cleanFundData.log',
                    filemode='a',
                    datefmt='%Y/%m/%d %H:%M:%S',
                    format='%(levelname)s %(asctime)s %(funcName)s %(lineno)d %(message)s'
                    )
logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())


# NAV切片数据统计
def calNAVStat(table):
    dateStat = table.count(axis=1)
    fundStat = table.count()
    col = list([50, 60, 70, 80, 90, 95])
    df = pd.DataFrame(np.zeros([2, 6]), columns=col, index=['dateStat', 'fundStat'])
    for i in col:
        df.loc['dateStat', i] = np.percentile(dateStat, i)
        df.loc['fundStat', i] = np.percentile(fundStat, i)
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    writer = pd.ExcelWriter('../out/2-NAVStat.xlsx', engine='xlsxwriter')
    # Convert the dataframe to an XlsxWriter Excel object.
    dateStat.to_excel(writer, sheet_name='dateStat')
    fundStat.to_excel(writer, sheet_name='fundStat')
    df.to_excel(writer, sheet_name='percentileStat')
    # Close the Pandas Excel writer and output the Excel file.
    writer.save()
    return dateStat, fundStat, df


# 处理NAV数据 TODO: columns
def processNAV(dbTableName, column, frequency='B'):
    logger.info('Processing NAV...')
    try:
        # process
        dbTable = pd.read_csv('../out/1-%s.csv' % dbTableName, dtype=str)
        dbTable['the_date'] = dbTable['the_date'].values.astype('datetime64[D]')
        dbTable[column] = pd.to_numeric(dbTable[column], errors='coerce')
        start = time.time()
        dbTable = dbTable.pivot('the_date', 'fund_code', column)
        dbTable = dbTable.fillna(method='ffill').resample(frequency).asfreq()
        end = time.time()
        msg = "Time used to pivot table and resample: " + str(end - start)
        logger.info(msg)
        # cal statistic
        calNAVStat(dbTable)
        # export
        dbTable.name = 'NAV'
        exportTable(dbTable,column+'-'+frequency)
        msg = str("NAV数据初步处理成功！[%s-%s-%s]" % (dbTableName, column, frequency))
        logger.info(msg)
        return dbTable
    except Exception as e:
        msg = str("NAV数据初步处理失败！\n%s" % e)
        logger.exception(msg)
        return 1


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
def calAnnualReturn(df,return_r,term,result):
    df[return_r] = df.apply(lambda x: x[return_r].strip('%').strip(), axis=1)
    df[return_r] = df[return_r].replace({'': 0})
    df[return_r] = df[return_r].replace({'-': 0})
    df[return_r] = df[return_r].replace({',': ''})
    df[result] = df.apply(
        lambda x: ((float(str(x[return_r]).replace(',', '')) / 100) + 1.0) ** (1 / x[term]) - 1.0 if x[term] != 0.00000 else 0,axis=1)
    return df


# 处理Manager数据
def processManager():
    # manager history
    logger.info('Processing manager history...')
    mana_his = pd.read_csv('../out/1-fund_managers_his.csv', dtype=str)
    # stupid case
    mana_his.loc[mana_his['manager_name'] == 'WANG AO(汪澳)', 'manager_name'] = '汪澳'
    mana_his.loc[mana_his['manager_name'] == 'TIAN HUAN', 'manager_name'] = 'TIAN_HUAN'
    mana_his.loc[mana_his['manager_name'] == 'DANIELDONGNINGSUN', 'manager_name'] = '孙东宁'
    mana_his.loc[mana_his['manager_name'] == 'IKEDA KAE', 'manager_name'] = 'IKEDA_KAE'
    # deal w till-now
    mana_his.loc[mana_his['end_date'] == '至今', 'end_date'] = mana_his.loc[mana_his['end_date'] == '至今', 'updated_date']
    mana_his['end_date'] = mana_his['end_date'].astype('datetime64[D]')
    # strip term of years
    mana_his = stripTerm(mana_his,'term','Y')
    # mana_his['term'] = mana_his['term'].replace(np.nan, "0天")
    # mana_his['term'] = mana_his.apply(
    #     lambda x: (int(x['term'].split('年又')[0]) + int(x['term'].split('年又')[1].strip('天')) / 365.25)
    #     if len(x['term'].split('年又')) > 1 else (int(x['term'].split('年又')[0].strip('天')) / 365.25), axis=1)
    # handle return rate
    mana_his = calAnnualReturn(mana_his,'return_rate','term','annual_return')
    # mana_his['return_rate'] = mana_his.apply(lambda x: x['return_rate'].strip('%').strip(), axis=1)
    # mana_his['return_rate'] = mana_his['return_rate'].replace({'': 0})
    # mana_his['return_rate'] = mana_his['return_rate'].replace({'-': 0})
    # mana_his['return_rate'] = mana_his['return_rate'].replace({',': ''})
    # mana_his['annual_return'] = mana_his.apply(
    #     lambda x: ((float(str(x['return_rate']).replace(',', '')) / 100) + 1.0) ** (1 / x['term']) - 1.0 if x['term'] != 0.00000 else 0,axis=1)
    # gen weight acc to year of data
    mana_his['annual_return_weight'] = mana_his.apply(
        lambda x: 1 - (2018 - x['end_date'].year) * 0.04 if x['end_date'].year >= 2008 else 0.55, axis=1)
    # 1\ scoring  on manager based on annual return rate
    mana_his['annual_return_score'] = mana_his.apply(
        lambda x: x['annual_return'] * x['annual_return_weight'] * x['term'], axis=1)
    mana_score = mana_his.groupby('manager_id').apply(
        lambda x: (x['annual_return'] * x['annual_return_weight'] * x['term']).sum() / x['term'].sum())  # interesting
    mana_score = pd.DataFrame(mana_score, columns=['annual_return_score'])
    # 2\ 资历（最早任职时间+最后任职时间）
    mana_score['earliest_start_date'] = mana_his.groupby(['manager_id'])['start_date'].agg('min').astype(
        'datetime64[D]')
    mana_score['latest_end_date'] = mana_his.groupby(['manager_id'])['end_date'].agg('max').astype('datetime64[D]')
    # 3\ 在行时间与比率
    mana_score['cum_on_duty_term'] = mana_his.groupby(['manager_id'])['cum_on_duty_term'].agg('max')
    # 时间（天）
    mana_score = stripTerm(mana_score,'cum_on_duty_term','D')
    # mana_score['cum_on_duty_term'] = mana_score.apply(
    #     lambda x: (int(int(x['cum_on_duty_term'].split('年又')[0]) * 365.25) + int(
    #         x['cum_on_duty_term'].split('年又')[1].strip('天')))if len(x['cum_on_duty_term'].split('年又')) > 1 else (int(x['cum_on_duty_term'].split('年又')[0].strip('天'))),axis=1)
    # 比率
    mana_score['total_term'] = mana_score.apply(lambda x: (x['latest_end_date'] - x['earliest_start_date']).days,axis=1)
    mana_score['cum_on_duty_term_pct'] = mana_score.apply(lambda x: x['cum_on_duty_term'] / x['total_term'], axis=1)
    # 4\ manager name
    mana_score = pd.merge(mana_score, mana_his[['manager_id', 'manager_name']], how='left', left_index=True,right_on='manager_id')

    # manager info
    logger.info('Processing manager info...')
    mana_info = pd.read_csv('../out/1-fund_managers_info.csv', dtype=str)
    del mana_info['manager_name']
    mana_info = pd.merge(mana_info, mana_score, how='left', on='manager_id')
    mana_info.name = 'manager_info'
    exportTable(mana_info)
    # filter and gen a list of manager todo move to modeling
    mana_list = mana_info.loc[(mana_info['annual_return_score'] >= 0.07) & (mana_info['cum_on_duty_term_pct'] >= 0.6),:]  # filter 1 and filter 2
    mana_list.to_csv('../out/2-managers_list.csv', index=False)

    # manager changes on fund
    logger.info('Processing manager chg on funds...')
    mana_chg = pd.read_csv('../out/1-fund_managers_chg.csv', dtype=str)
    # stupid case again
    mana_chg['fund_managers'] = mana_chg['fund_managers'].apply(lambda x: x.replace('WANG AO(汪澳)', '汪澳'))
    mana_chg['fund_managers'] = mana_chg['fund_managers'].apply(lambda x: x.replace('TIAN HUAN', 'TIAN_HUAN'))
    mana_chg['fund_managers'] = mana_chg['fund_managers'].apply(lambda x: x.replace('DANIEL DONGNING SUN', '孙东宁'))
    mana_chg['fund_managers'] = mana_chg['fund_managers'].apply(lambda x: x.replace('DANIELDONGNINGSUN', '孙东宁'))
    mana_chg['fund_managers'] = mana_chg['fund_managers'].apply(lambda x: x.replace('IKEDA KAE', 'IKEDA_KAE'))
    mana_chg['fund_managers'] = mana_chg['fund_managers'].apply(lambda x: x.replace('SHI CHENG(史程)', '史程'))
    mana_chg['fund_managers'] = mana_chg['fund_managers'].apply(lambda x: x.replace('IKEDA KAE', 'IKEDA_KAE'))
    del mana_chg['created_date']
    # get only latest record of each fund
    mana_chg.loc[mana_chg['end_date'] == '至今', 'end_date'] = mana_chg.loc[mana_chg['end_date'] == '至今', 'updated_date']
    mana_chg['end_date'] = mana_chg['end_date'].astype('datetime64[D]')
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
    # p_chg['term'] = p_chg.apply(
    #     lambda x: (int(x['term'].split('年又')[0]) + int(x['term'].split('年又')[1].strip('天')) / 365.25)
    #     if len(x['term'].split('年又')) > 1 else (int(x['term'].split('年又')[0].strip('天')) / 365.25), axis=1)
    # gen annual return rate based on funds
    p_chg = calAnnualReturn(p_chg,'return_rate','term','annual_return_fund')
    # p_chg['return_rate'] = p_chg.apply(lambda x: x['return_rate'].strip('%').strip(), axis=1)
    # p_chg['return_rate'] = p_chg['return_rate'].replace({'': 0})
    # p_chg['annual_return_fund'] = p_chg.apply(
    #     lambda x: ((float(x['return_rate']) / 100) + 1.0) ** (1 / x['term']) - 1.0 if x['term'] != 0.00000 else 0,
    #     axis=1)
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
    # chk = p_chg[p_chg['total_term'] == 'nan']
    p_chg.name = 'manager_chg'
    exportTable(p_chg)
    logger.info('Done processing MANAGER!')

    # todo move to modeling
    var = p_chg.loc[(p_chg['annual_return_fund'] >= 0.15) & (p_chg['term'] >= 2)&(p_chg['weighted_annual_return_score']>=0.05), :] # filter 3 4 5


# 导出表格为csv
def exportTable(tab,mark = None):
    try:
        if mark is not None:
            tab.to_csv('../out/2-%s-%s.csv' % (tab.name,mark), index=False)
        else:
            tab.to_csv('../out/2-%s.csv' % tab.name, index=False)
        msg = str('EXPORT TABLES %s Succeed!'% tab.name)
        logger.info(msg)
    except Exception as e:
        msg = str('EXPORT TABLES %s Fail:\n%s' % (tab.name,e))
        logger.exception(msg)


def main():
    # # NAV
    # # tableName,columnName,frequency
    # nav = processNAV('fund_nav', 'nav', 'B')
    # # dateStat, fundStat, dbStat = calNAVStat(nav)
    # # dbRe = nav.resample('BM').asfreq()
    #
    # MANAGER
    processManager()

    # # fund_info
    # dbTableName = 'fund_info'
    # dbTable = pd.read_csv('../out/1-%s.csv' % dbTableName, dtype=str)
    # fund_info = dbTable[['fund_code', 'fund_name', 'fund_abbr_name', 'fund_type',
    #                      'issue_date', 'establish_date', 'establish_scale', 'asset_value',
    #                      'asset_value_date', 'units', 'units_date', 'fund_manager',
    #                      'fund_trustee', 'funder', 'total_div', 'mgt_fee', 'trust_fee',
    #                      'sale_fee', 'buy_fee', 'buy_fee2', 'benchmark', 'created_date']]
    # mana = fund_info['funder'].apply(lambda x: x.split('、'))



    # TODO: fund_info ??
    # TODO: cum_nav


if __name__ == "__main__":
    main()
