# -*- coding: utf-8 -*-
"""
Created on Sun Nov 25 16:16:10 2018

@author: JON7390
交易一期(基金)模型
"""
import itertools
import pickle as pk
from multiprocessing import Lock, Process, Queue, cpu_count

import cvxopt as opt
import yaml
from cvxopt import blas, solvers
from scipy.special import comb
from sqlalchemy import create_engine

from utils import logger as lg
from utils.decorator import *
from utils.utils import *

solvers.options['show_progress'] = False  # Turn off progress printing
logger = lg.init_logger('./log/3-genPort_%s.log' % dt.now().strftime('%Y-%m-%d'))


class HarryMarkowitz:
    def __init__(self, cfg_path='config.yaml'):
        config = yaml.load(open(cfg_path))

        # global params
        global risk_free, fit_frequency, portfolio_nbr, proc_nbr, lock_in, lock_out
        [self.risk_free, self.fit_frequency, self.portfolio_nbr, self.ytg] = [*config['Portfolio'].values()]
        self.proc_nbr = int(input('Number of process: '))
        self.lock_in = Lock()
        self.lock_out = Lock()

        # local params
        db_con = config['MySQL']
        self.engine = create_engine(
            "mysql://%s:%s@%s/%s?charset=utf8mb4" % (db_con['user'], db_con['passwd'], db_con['host'], db_con['db']))
        self.nav_file = config['NAVFile']

    def select_train_date(self):
        self.train_date = select_date(self.engine)
        return self.train_date

    def loadData(self, cur_date):
        """load processed data"""
        try:
            self.filtered_nav = pk.load(open(self.nav_file['res_path'] % cur_date, 'rb'))
            logger.info(f'loaded filtered NAV data- {str(self.filtered_nav.shape)}')
        except:
            logger.error("RUN DATA PROCESS PART BEFORE THIS!!")
            sys.exit()

    @time_elapsed
    def preciseCorvariance(self, cur_date):
        """calculate precise corvariance"""
        try:
            corMat = pk.load(open(self.nav_file['cor_path'] % cur_date, 'rb'))
        except:
            starting = self.filtered_nav.index[-1] - relativedelta(years=self.ytg)
            df = self.filtered_nav.copy()

            df = df.fillna(method='ffill').pct_change()
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

            pk.dump(corMat, open(self.nav_file['cor_path'] % cur_date, 'wb'), True)

        self.pre_cor_nav = corMat

    def fitHM(self):
        self.pchg_nav = fitHM(self.filtered_nav, self.fit_frequency, self.ytg)

    @time_elapsed
    def harryMarkowitzMP(self, nbr):
        # global risk_free, fit_frequency, lock_in, lock_out, engine

        self.in_queue = Queue()  # multiprocess -> input queue
        self.out_queue = Queue()  # multiprocess -> output queue
        self.proc_list = []

        logger.info('\n%d CPU on board, starting %d process' % (cpu_count(), self.proc_nbr))

        # gen combination & input queue
        for col in itertools.combinations(self.pchg_nav.columns, nbr):
            self.in_queue.put(col)
        logger.info('%d combination in queue' % self.in_queue.qsize())

        # create calculating processes
        for w in range(self.proc_nbr - 1):
            p = Process(target=calculating_proc,
                        args=(
                            nbr, self.pchg_nav, self.pre_cor_nav, self.in_queue, self.out_queue, self.lock_in,
                            self.lock_out, self.risk_free, self.fit_frequency))
            self.proc_list.append(p)

        # create formating processes
        self.proc_list.append(
            Process(target=formating_proc,
                    args=(nbr, self.train_date, self.out_queue, self.risk_free, self.fit_frequency)))

        ## starting process
        [p.start() for p in self.proc_list]

        ## completing process
        [p.join() for p in self.proc_list]


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


def harryMarkowitzOptimizer(navReturn, nbr, pre, risk_free, fit_frequency):  # harry markoviz optimizer
    # Get navReturn column headers
    header = list(navReturn.columns.values)

    # Convert to cvxopt matricess
    navReturn = np.asmatrix(navReturn[1:].T.values)  # del first columns of NAN
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

    # scale desired returns
    N = 25
    mus = [10 ** (4 * t / N - 2) for t in range(N)]  # desired portfolio returns

    # Calculate [efficient frontier] weights using quadratic programming
    portfolios = [solvers.qp(mu * S, -pbar, G, h, A, b)['x'] for mu in mus]
    # x-> weighted portfolios| solvers.qp(P,q,G,h,A,b)

    # CALCULATE RISKS AND RETURNS FOR FRONTIER
    returns = [blas.dot(pbar, x) for x in portfolios]
    risks = [np.sqrt(blas.dot(x, S * x)) for x in portfolios]
    risk_return = pd.DataFrame({'risks': risks, 'returns': returns})

    # FIT THE 2ND DEGREE POLYNOMIAL OF THE FRONTIER CURVE
    m1 = np.polyfit(returns, risks, 2)  # polyfit: x,y,degree

    # CALCULATE 3 OUTPUT:  MAXIMUM SHARPE RATIO/ DEFAULT/ MINIMUM RISK
    # MAXIMUM SHARPE RATIO
    srp = sharpeRatio(risk_return, None, risk_free, fit_frequency, False)
    srp_y = float(srp.loc[srp['sharpeRatio'] == srp['sharpeRatio'].max(), 'returns'])
    # srp_y = srp[0] if isinstance(srp_y,seri) else srp_y # TODO AVOID MULTIPLE RESULT
    # MINIMUM RISK
    min_y = -m1[1] / (2 * m1[0])  # -(b/2a)
    # DEFAULT
    if (m1[2] / m1[0]) < 0:
        default_y = min_y
    else:
        default_y = np.sqrt(m1[2] / m1[0])
    port = []
    for i in [srp_y, default_y, min_y]:
        # CALCULATE THE [OPTIMAL PORTFOLIO WEIGHT] with min risk
        op_wt = solvers.qp(opt.matrix(i * S), -pbar, G, h, A, b)['x']
        op_return = blas.dot(pbar, op_wt)  # return under OPTIMAL PRTFOLIO
        op_risk = np.sqrt(blas.dot(op_wt, S * op_wt))  # risk under OPTIMAL PORTFOLIO
        op_wt_list = []
        [op_wt_list.append(i) for i in header]
        [op_wt_list.append(i) for i in op_wt]
        [op_wt_list.append(i) for i in [op_return, op_risk]]
        port.append(op_wt_list)
    return port


@time_elapsed
def calculating_proc(nbr, return_vec, preCor, in_queue, out_queue, lock_in, lock_out, risk_free, fit_frequency):
    """multiprocess -> input queue + function + output queue"""
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
            chk_relevance = epo_cor.apply(lambda x: 1 if (x >= 0.3).any() else 0).sum()
            if chk_relevance > 0:
                logger.warning(f"\n本组合相关性过大：{str(col)}")
                continue
        # train and output
        try:
            port = harryMarkowitzOptimizer(epo_return, nbr, epo_cor, risk_free, fit_frequency)  # optimal solver
            lock_out.acquire()
            [out_queue.put(i) for i in port]
            lock_out.release()
            out_count = out_count + 1
        except Exception:
            logger.exception(f"\n本组合计算失败：{str(col)}")

    logger.info('[%s] in-%d | out-%d - exit calculating process' % (str(os.getpid()), in_count, out_count))


def formating_proc(nbr, train_date, out_queue, risk_free, fit_frequency):
    for_count = 0
    awake = 0

    result_header = []
    [result_header.append(f'fundCode_{i}') for i in range(nbr)]
    [result_header.append(f'portfolio_{i}') for i in range(nbr)]
    [result_header.append(i) for i in ['returns', 'risks']]
    port_all = pd.DataFrame(columns=result_header)

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

            if out_queue.qsize() < 2000:
                epoch = 1
            else:
                epoch = 2000
            id_port_return_risk = []
            for i in range(3 * epoch):
                for_count = for_count + 1
                id_port_return_risk.append(out_queue.get())
            port_res = pd.DataFrame(id_port_return_risk, columns=result_header)

            # concat fund code & portfolio & return & risk
            port_all = pd.concat([port_all, port_res], axis=0, ignore_index=True)
            # verbose
            if for_count % 100 == 0:
                logger.info('formatting portfolio %d，%d left' % (for_count, out_queue.qsize()))

    logger.info('tuning portfolio')

    # common columns
    port_all.loc[0::3, 'label'] = "srp"
    port_all.loc[1::3, 'label'] = "def"
    port_all.loc[2::3, 'label'] = "min"
    port_all['train_date'] = str(train_date)[:10]

    # calculate sharpe ratio
    port_all = sharpeRatio(port_all, None, risk_free, fit_frequency, False)

    # drop duplicate entries
    port_all = port_all.round(1)
    port_all['unique_flag'] = port_all.apply(lambda x: int(x[0]) * x[3] + int(x[1]) * x[4] + int(x[2]) * x[5], axis=1)
    port_all = port_all.iloc[port_all['unique_flag'].drop_duplicates().index, :-1]
    logger.info('%d portfolio generated' % port_all.shape[1])

    logger.info('exporting portfolio')

    # create database engine
    config = yaml.load(open('config.yaml'))
    mySQL = config['MySQL']
    engine = create_engine("mysql://%s:%s@%s/%s" % (mySQL['user'], mySQL['passwd'], mySQL['host'], mySQL['db']))

    # export params
    param = pd.DataFrame({**config['ManagerFilter'], **config['Portfolio']}, index=[0])
    param['batchid'] = str(dt.now().strftime('%Y%m%d%H'))
    pk.dump(param, open('./data/3-PARAMS_%s.dat' % str(train_date)[:10], 'wb'), True)
    # param = pk.load(open('./data/3-PARAMS_%s.dat' % train_date, 'rb'))
    param.to_sql(name='params', con=engine, if_exists='append', index=False)

    # export portfolio to disk & database
    port_all['batchid'] = str(dt.now().strftime('%Y%m%d%H'))
    port_all.to_csv('./data/3-PORT_%d_%s.csv' % (nbr, str(dt.now().strftime('%Y%m%d'))), index=False)
    pk.dump(port_all, open('./data/3-PORT_%s.dat' % str(train_date)[:10], 'wb'), True)
    # port_all = pk.load(open('./data/3-PORT_%s.dat' % train_date, 'rb'))
    port_all.to_sql(name='fund_portfolio_3', con=engine, if_exists='append', index=False)


def main():
    # init
    hm = HarryMarkowitz()

    # train date
    train_date = hm.select_train_date()
    cur_date = str(train_date)[:10]

    # load processed data
    hm.loadData(cur_date)

    # gen return of interest
    hm.fitHM()

    # # gen precise corvariance
    hm.preciseCorvariance(cur_date)

    # gen portfolio
    [hm.harryMarkowitzMP(nbr) for nbr in hm.portfolio_nbr]

    logger.info('ALL DONE')


if __name__ == "__main__":
    main()
