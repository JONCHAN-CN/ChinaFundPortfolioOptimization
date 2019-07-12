import yaml

from utils import PyMySQL
from utils import logger

logger = logger.init_logger()
cfp = yaml.load(open('./dep/config.yaml'))
db = [*cfp['MySQL'].values()]


def main():
    global mySQL, request_sleep, isproxy, proxy, header, fundSpiders, inQueue, outQueue, lock, process_finish, process_sleep, fund_count, count
    mySQL = PyMySQL.PyMySQL()
    mySQL._init_(*db)

    # get portfolio from database
    get_port = 'select distinct * from fund_portfolio_3 where expire_date = null'
    portfolio = mySQL.sql(get_port)

    # run with current nav get a act_returns from now
    # portfolio['act_returns']
    # plot data

    # judge if any entry should be expired

    # make expired entries

    # import result into table

    # cal accuracy of the model by date and tell


if __name__ == "__main__":
    main()
