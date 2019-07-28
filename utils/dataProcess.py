# -*- coding: utf-8 -*-
"""
Created on Mon Nov  5 19:02:10 2018

@author: JON7390
交易一期(基金)数据处理
"""
import logging
import os

import pandas as pd

from .decorator import try_exception
from .utils import calNAVStat, exportTable, stripTerm, calAnnualReturn, replaceManaName

logger = logging.getLogger('main.DataProcess')


@try_exception
def processManaHis(mana_his):
    logger.info('\nProcessing manager history...')

    # clean weired name case
    mana_his = replaceManaName(mana_his, 'manager_name')

    # deal w till-now
    mana_his.loc[mana_his['end_date'] == '至今', 'end_date'] = mana_his.loc[mana_his['end_date'] == '至今', 'updated_date']
    mana_his['end_date'] = mana_his['end_date'].astype('datetime64[D]')

    # strip term of years
    mana_his = stripTerm(mana_his, 'term')

    # handle return rate
    mana_his = calAnnualReturn(mana_his, return_rate='return_rate', term='term', result='annual_return')

    # gen weight acc to year of data
    weight_lambda = lambda x: 1 - (2019 - x['end_date'].year) * 0.04 if x['end_date'].year >= 2008 else 0.52
    mana_his['annual_return_weight'] = mana_his.apply(weight_lambda, axis=1)

    return mana_his


@try_exception
def processManaChg(mana_chg):
    logger.info('\nProcessing manager chg on funds...')
    mana_chg.drop(columns=['created_date'], inplace=True)

    # clean weired name case
    mana_chg = replaceManaName(mana_chg, 'fund_managers')

    # deal w till-now
    mana_chg.loc[mana_chg['end_date'] == '至今', 'end_date'] = mana_chg.loc[mana_chg['end_date'] == '至今', 'updated_date']
    mana_chg['end_date'] = mana_chg['end_date'].astype('datetime64[D]')

    # get latest record of each fund
    mana_chg = mana_chg.iloc[mana_chg.groupby(['fund_code'])['end_date'].agg('idxmax')]

    # gen a tall table based on managers
    mana_chg = pd.merge(mana_chg, mana_chg['fund_managers'].str.split(' ', expand=True), how='left', left_index=True,
                        right_index=True)

    return mana_chg


@try_exception
def processManaInfo(mana_info, mana_score):
    logger.info('\nProcessing manager info...')
    # merge
    mana_info.drop(columns=['manager_name'], inplace=True)
    mana_info = pd.merge(mana_info, mana_score, how='left', on='manager_id')
    mana_info.drop_duplicates(inplace=True)

    # export
    name = 'MANAGER_info'
    exportTable(mana_info, name=name, freq=None, index_flag=False)

    return mana_info


@try_exception
def genManaScore(mana_his):
    logger.info('Calculating manager score...')

    # 1\ 能力(annual return rate)
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
    mana_score = stripTerm(mana_score, 'cum_on_duty_term', 'D')  # 时间（天）
    mana_score['cum_on_duty_term_pct'] = mana_score.apply(lambda x: x['cum_on_duty_term'] / x['total_term'],
                                                          axis=1)  # 比率
    # 4\ manager name
    mana_score = pd.merge(mana_score, mana_his[['manager_id', 'manager_name']], how='left', left_index=True,
                          right_on='manager_id')
    mana_score.drop_duplicates(inplace=True)

    return mana_score


@try_exception
def mergeMana(mana_chg, mana_his, mana_info):
    p_chg = pd.DataFrame()

    # gen tall table based on single manager
    for i in range(0, 3):
        exclude = list([0, 1, 2, 3])
        exclude.pop(i)
        tmp = mana_chg.loc[mana_chg[i].astype('str') != 'None', ~mana_chg.columns.isin(exclude)]
        tmp.rename(columns={i: 'single_manager'}, inplace=True)
        p_chg = pd.concat([p_chg, tmp], axis=0)

    # strip term of years
    p_chg = stripTerm(p_chg, 'term')

    # gen annual return rate based on funds
    p_chg = calAnnualReturn(p_chg, 'return_rate', 'term', 'annual_return_fund')

    # merge mana_his and mana_info
    tmp = mana_his[['manager_id', 'fund_code', 'manager_name']].drop_duplicates()
    p_chg = pd.merge(p_chg, tmp, how='left', left_on=['single_manager', 'fund_code'],
                     right_on=['manager_name', 'fund_code'])
    tmp = mana_info[['manager_id', 'annual_return_score', 'total_term', 'cum_on_duty_term_pct']].drop_duplicates()
    p_chg = pd.merge(p_chg, tmp, how='left', left_on=['manager_id'], right_on=['manager_id'])

    # fill nan annual return score based on managers with annual return rate of fund
    p_chg['annual_return_score'] = p_chg.apply(
        lambda x: x['annual_return_fund'] if str(x['annual_return_score']) == 'nan' else x['annual_return_score'],
        axis=1)

    # gen weighted annual return score based on managers
    p_chg['weighted_annual_return_score'] = p_chg.groupby(['fund_code'])['annual_return_score'].transform('mean')

    # p_chg['total_term'] = p_chg['total_term'].apply(lambda x:str(x))
    # count_down = p_chg[p_chg['total_term'] == 'nan']
    p_chg.drop_duplicates(inplace=True)

    # eport
    name = 'MANAGER_chg'
    exportTable(p_chg, name=name, freq=None, index_flag=False)

    return p_chg


def processManager(mana_his_path, mana_info_path, mana_chg_path, res_mana_chg_path, res_mana_info_path):
    '''load/ clean/ process manager data'''
    if os.path.exists(res_mana_chg_path) and os.path.exists(res_mana_info_path):
        # load previous data
        p_chg = pd.read_csv(res_mana_chg_path, dtype=str)
        mana_info = pd.read_csv(res_mana_info_path, dtype=str)
    else:
        # load manager data
        mana_his = pd.read_csv(mana_his_path, dtype=str)
        mana_info = pd.read_csv(mana_info_path, dtype=str)
        mana_chg = pd.read_csv(mana_chg_path, dtype=str)

        # manager history
        mana_his = processManaHis(mana_his)

        # generate manager score
        mana_score = genManaScore(mana_his)

        # manager basic info w/ score
        mana_info = processManaInfo(mana_info, mana_score)

        # manager changes on fund
        mana_chg = processManaChg(mana_chg)

        # merge all data
        p_chg = mergeMana(mana_chg, mana_his, mana_info)

        logger.info('\nDone processing MANAGER!')
    return mana_info, p_chg


@try_exception
def processNAV(df, frequency='BM', export=False):
    '''clean/ process NAV data according to frquency'''
    logger.info('Processing NAV...')
    df['date'] = df['date'].astype('datetime64[D]')

    # process
    name = 'NAV'
    df['nav'] = df['nav'].astype('float')  # TODO how to deal w/ div_rec
    df = df.pivot('date', 'fund_code', 'nav').fillna(method='ffill').resample(frequency).asfreq()

    # cal statistic
    calNAVStat(df, name)

    # export table
    if export:
        exportTable(df, name=name, freq=frequency, index_flag=False)

    logger.info(str('\nDone processing - [%s-%s]' % (name, frequency)))
    return df


@try_exception
def processCUR(df, frequency='BM', export=False):
    '''clean/ process currency NAV data according to frquency'''
    logger.info('Processing currency NAV...')
    df['date'] = df['date'].astype('datetime64[D]')

    # process
    name = 'CURRENCY'
    df['profit_rate'] = df['profit_rate'].apply(lambda x: str(x).strip('%')).fillna(0).astype('float') / 100
    df['date_m'] = df['date'].apply(lambda x: str(x)[:7])
    df = df.groupby(['fund_code', 'date_m'], as_index=False)['profit_rate'].mean()
    df = df.pivot('date_m', 'fund_code', 'profit_rate').fillna(method='ffill')
    df = (df / 12) if 'M' in frequency else df
    df = (df / 4) if 'Q' in frequency else df

    # cal statistic
    calNAVStat(df, name)

    # export table
    if export:
        exportTable(df, name=name, freq=frequency, index_flag=False)

    logger.info(str('\nDone processing - [%s-%s]' % (name, frequency)))
    return df