# -*- coding: utf-8 -*-
"""
Created on Mon Nov  5 19:02:10 2018

@author: JON7390
交易一期(基金)数据处理
"""

import itertools
import pickle as pk

import yaml
from scipy.special import comb
from sqlalchemy import create_engine

from utils import logger as lg
from utils.decorator import *
from utils.utils import *

logger = lg.init_logger()


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
    name = 'MANAGER_INFO'
    exportDataframe(mana_info, name=name, freq=None, index_flag=False)

    return mana_info


@try_exception
def genManaScore(mana_his):
    logger.info('Calculating manager score...')

    # gen weight acc to year of data
    cur_year = int(dt.now().strftime('%Y'))
    weight_lambda = lambda x: 1 - (cur_year - x['end_date'].year) * 0.05 if x['end_date'].year >= 2013 else 0.5
    mana_his['annual_return_weight'] = mana_his.apply(weight_lambda, axis=1)

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
    name = 'MANAGER_CHG'
    exportDataframe(p_chg, name=name, freq=None, index_flag=False)

    return p_chg


def processManager(mana_his_path, mana_info_path, mana_chg_path, res_mana_chg_path, res_mana_info_path):
    """load/ clean/ process manager data"""
    if os.path.exists(res_mana_chg_path) and os.path.exists(res_mana_info_path):
        # load previous data
        mana_chg = pd.read_csv(res_mana_chg_path, dtype=str)
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
        mana_chg = mergeMana(mana_chg, mana_his, mana_info)

        logger.info('\nDone processing MANAGER!')
    return mana_info, mana_chg


@try_exception
def processCUR(df, frequency='BM', export=False):
    """process currency NAV data according to frquency"""
    logger.info('processing CUR')

    # process
    df['date'] = df['date'].astype('datetime64[D]')
    df['profit_rate'] = df['profit_rate'].apply(lambda x: str(x).strip('%')).fillna(0).astype('float') / 100
    # df['date_m'] = df['date'].apply(lambda x: str(x)[:7])
    # df = df.groupby(['fund_code', 'date_m'], as_index=False)['profit_rate'].mean()
    df = df.pivot('date', 'fund_code', 'profit_rate').fillna(method='ffill').resample(frequency).mean()
    # df = (df / 52) if 'W' in frequency else df
    # df = (df / 12) if 'M' in frequency else df
    # df = (df / 4) if 'Q' in frequency else df

    # cal statistic
    calNAVStat(df, "CUR")

    # export table
    if export:
        exportDataframe(df, name="CUR", freq=frequency, index_flag=False)

    logger.info(str('\nDone processing - [%s-%s]' % ("CUR", frequency)))
    return df


@try_exception
def genNAVScore(p_nav):
    logger.info('Calculating NAV score...')

    p_nav_score = p_nav.copy()
    _lbd = lambda x: (x == 0.0).sum()
    p_nav_score['isZero'] = p_nav.apply(_lbd, axis=1)
    _lbd = lambda x: (x.abs() >= 0.025).sum()
    p_nav_score['return_2.5'] = p_nav.apply(_lbd, axis=1)
    _lbd = lambda x: (x.abs() >= 0.05).sum()
    p_nav_score['return_5'] = p_nav.apply(_lbd, axis=1)
    _lbd = lambda x: (x <= 0.0).sum()
    p_nav_score['return_negative'] = p_nav.apply(_lbd, axis=1)
    _lbd = lambda x: x.mean()
    p_nav_score['return_mean'] = p_nav.apply(_lbd, axis=1)
    _lbd = lambda x: x.max()
    p_nav_score['return_max'] = p_nav.apply(_lbd, axis=1)
    _lbd = lambda x: x.min()
    p_nav_score['return_min'] = p_nav.apply(_lbd, axis=1)
    _lbd = lambda x: x.var()
    p_nav_score['return_variance'] = p_nav.apply(_lbd, axis=1)
    _lbd = lambda x: x.std()
    p_nav_score['return_std'] = p_nav.apply(_lbd, axis=1)

    return p_nav_score


@try_exception
def processNAV(df, frequency='BM', export=False):
    """process NAV data according to frquency"""
    logger.info('processing NAV')

    # process
    df['date'] = df['date'].astype('datetime64[D]')
    df['add_nav'] = pd.to_numeric(df['add_nav'], errors='coerce')
    df = df.pivot('date', 'fund_code', 'add_nav').fillna(method='ffill').resample(frequency).asfreq()
    p_df = fitHM(df, frequency=frequency)
    nav_score = genNAVScore(p_df.T)

    # cal statistic
    calNAVStat(df, "NAV")

    # export table
    if export:
        exportDataframe(df, name="NAV", freq=frequency, index_flag=False)

    logger.info(str('\nDone processing - [%s-%s]' % ("NAV", frequency)))
    return df, nav_score


@time_elapsed
def preciseCorvariance(nav, years):
    """calculate precise corvariance"""
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


def filterNAV(nav, nav_score, isZero_coe=0.8, return_mean_coe=0.2, return_25_coe=0.2, mode='auto', ):
    logger.info('Filtering fund list on NAV...')

    # del all nan & inf data
    nav_score_nan = nav_score.loc[(nav_score.return_mean.isna()) & (nav_score.return_mean == np.float('inf'))]
    exportDataframe(nav_score_nan, name="NAV-NAN", index_flag=True)
    nav_score = nav_score.loc[(~nav_score.return_mean.isna()) & (nav_score.return_mean != np.float('inf'))]

    if mode == "auto":
        filter_isZero = nav_score.shape[1] * isZero_coe
        nav_score['filter_isZero'] = nav_score.apply(lambda x: 0 if x['isZero'] >= filter_isZero else 1, axis=1)
        filter_return_mean = nav_score['return_mean'].mean() * return_mean_coe
        nav_score['filter_return_mean'] = nav_score.apply(lambda x: 1 if x['return_mean'] >= filter_return_mean else 0,
                                                          axis=1)
        filter_return_25 = nav_score['return_2.5'].mean() * return_25_coe
        nav_score['filter_return_2.5'] = nav_score.apply(lambda x: 1 if x['return_2.5'] >= filter_return_25 else 0,
                                                         axis=1)

        # log filter application
        for i in list(['filter_isZero', 'filter_return_mean', 'filter_return_2.5']):
            cnt = pd.Series(nav_score.loc[nav_score[i] >= 1].index.unique()).count()
            logger.info('After filterManager %s - %s:' % (i, str(cnt)))

        con = (nav_score[['filter_isZero', 'filter_return_mean', 'filter_return_2.5']].sum(axis=1) >= 3)
        fund_list = nav_score[con]

        logger.info('After ALL FILTER:' + str(len(fund_list.index.unique())))
        return fund_list.index


def filterManager(mana_info, mana_chg, annual_return_score=0.075, cum_on_duty_term_pct=0.55, annual_return_fund=0.075,
                  term=1.0, weighted_annual_return_score=0.075, mode='loose'):
    logger.info('Filtering fund list on manager...')

    mana_chg['manager_id'].replace({np.nan: 0}, inplace=True)

    # generate filter of manager based on manager
    return_score = mana_info[mana_info['annual_return_score'] >= annual_return_score][['manager_name', 'manager_id']]
    term_pct = mana_info[mana_info['cum_on_duty_term_pct'] >= cum_on_duty_term_pct][['manager_name', 'manager_id']]

    # apply filter of manager based on manager
    _lbd = lambda x: 1 if (str(x['manager_id']) in list(return_score['manager_id'])) else 0
    mana_chg['return_score'] = mana_chg.apply(_lbd, axis=1)
    _lbd = lambda x: 1 if (str(x['manager_id']) in list(term_pct['manager_id'])) else 0
    mana_chg['term_pct'] = mana_chg.apply(_lbd, axis=1)

    # apply filter of manager based on fund
    _lbd = lambda x: 1 if float(x['annual_return_fund']) >= annual_return_fund else 0
    mana_chg['return_rate'] = mana_chg.apply(_lbd, axis=1)
    mana_chg['term'] = mana_chg.apply(lambda x: 1 if float(x['term']) >= term else 0, axis=1)
    _lbd = lambda x: 1 if float(x['weighted_annual_return_score']) >= weighted_annual_return_score else 0
    mana_chg['weighted'] = mana_chg.apply(_lbd, axis=1)

    # log filter application
    for i in list(['return_score', 'term_pct', 'return_rate', 'term', 'weighted']):
        cnt = pd.Series(mana_chg.loc[mana_chg[i] >= 1, 'fund_code'].unique()).count()
        logger.info('After filterManager %s - %s:' % (i, str(cnt)))

    # generate fund list
    fund_list = pd.DataFrame()
    if mode == 'loose':
        con_1 = (mana_chg[['return_score', 'term_pct', 'return_rate', 'term']].sum(axis=1) >= 4)
        con_2 = (mana_chg[['term_pct', 'return_rate', 'term', 'weighted']].sum(axis=1) >= 4)
        fund_list = mana_chg[con_1 | con_2]
    elif mode == 'strict':
        con_3 = (mana_chg[['return_score', 'term_pct', 'return_rate', 'term', 'weighted']].sum(axis=1) >= 5)
        fund_list = mana_chg.loc[con_3]
    else:
        return 1

    logger.info('After ALL FILTER:' + str(len(fund_list['fund_code'].unique())))
    return fund_list['fund_code']


def main():
    config = yaml.load(open('config.yaml'))

    # local params
    db_con = config['MySQL']
    engine = create_engine("mysql://%s:%s@%s/%s?charset=utf8mb4" \
                           % (db_con['user'], db_con['passwd'], db_con['host'], db_con['db']))
    [fit_frequency, ytg] = [*config['DataProcess'].values()]
    mana_filter = config['ManagerFilter']
    fund_filter = config['FundFilter']
    nav_file = config['NAVFile']
    mana_file = [*config['ManagerFile'].values()]

    # train date
    train_date = select_date(engine)
    cur_date = str(train_date)[:10]
    start_date = str(train_date - relativedelta(years=ytg))

    # process data
    if os.path.exists(nav_file['res_path'] % cur_date):
        logger.info('PROCESSED DATA EXISTED')
    else:
        _sql = 'select distinct * from %s where date <=\'%s\' and date >=\'%s\''

        # nav
        nav = pd.read_sql_query(_sql % ('nav', train_date, start_date), engine)
        nav, nav_score = processNAV(nav, fit_frequency, True)  # adjusted prices

        ## currency nav
        # cur = pd.read_sql_query(_sql % ('nav_currency', train_date,start_date), engine)
        # cur = dataProcess.processCUR(cur, fit_frequency, True)  # annualised return rate by frequency

        # manager
        mana_info, mana_chg = processManager(*mana_file)

        # filter
        filtered_nav_list = filterNAV(nav, nav_score, **fund_filter)  # fund list after filtering NAV
        filtered_mana_list = filterManager(mana_info, mana_chg, **mana_filter)  # fund list after filtering managers
        filtered_nav = nav[list(set(filtered_mana_list) & set(filtered_nav_list))]

        # dump
        filtered_nav = filtered_nav.T.drop_duplicates().T
        logger.info(f'Saving filtered NAV data- {str(filtered_nav.shape)}')
        pk.dump(filtered_nav, open(nav_file['res_path'] % cur_date, 'wb'), True)

    logger.info('DATA PROCESS ALL DONE')


if __name__ == "__main__":
    main()
