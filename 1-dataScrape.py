# -*- coding:utf-8 -*-
"""
原PO: https://blog.csdn.net/yuzhucu/article/details/55261024
原作：yuzhucu
功能：抓取东方财富网上基金相关数据

@author: JON7390
交易一期(基金)数据
"""

import os
import queue
import socket
import sys
import threading
import time
from datetime import datetime as dt

import pandas as pd
import requests
import urllib3
import yaml
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

from utils import PyMySQL, logger, utils
from utils.utils import exportQuery

logger = logger.init_logger('./log/1-scrapeData_%s.log' % dt.now().strftime('%Y-%m-%d'))
cfp = yaml.load(open('./config.yaml', 'r'))
fund_file_path = "./dep/1-fundCode&Name.csv"
db = [*cfp['MySQL'].values()]  # unpack dict to get dict value


class FundSpiders():
    def getCurrentTime(self):
        """获取当前时间"""
        return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

    def updateFundCodesFromCsv(self):
        # get all possible code
        allCode = []
        for i in range(0, 1000000):
            len_ = len(str(i))
            fund_code = '%s%d' % ('0' * (6 - len_), i)
            allCode.append(fund_code)
        # cv with exisisting code
        existedCode = list(pd.DataFrame(mySQL.selectDistinct('fund_info'))['fund_code'])
        leftCode = set(allCode).difference(set(existedCode))
        # cv with done and not fund code
        with open("./dep/0-doneFundCode.txt", 'r') as f:
            doneCode = f.readlines()
        doneCode = list(map(str.strip, doneCode))
        leftCode = leftCode.difference(set(doneCode))
        # get new code with MP
        start = time.time()
        fund_count = len(list(leftCode))
        logger.info(f'Start update fund code list from {fund_count} codes')
        # create process queue ->fund_code/ func/ arg/ max retry time
        [inQueue.put([i]) for i in list(leftCode)]
        threads = [baseThread(processWorker_simple) for _ in range(8)]  # create process thread
        threads.append(baseThread(IOWorker_simple))  # create IO thread
        [thread.start() for thread in threads[:-1]]  # start process thread
        time.sleep(30)
        threads[-1].start()  # start IO thread
        [thread.join() for thread in threads]  # wait until all process finishes tasks
        logger.info('Time used multithreading scraping Fund Info: %s' % str(time.time() - start))
        # 　update code list
        _sql = 'DELETE FROM fund.fund_info where fund_name = \'---\' or fund_name is NULL;'
        mySQL.sql(_sql)
        all = pd.DataFrame(mySQL.selectDistinct('fund_info'))[['fund_code', 'fund_name']]
        all.to_csv("./dep/1-fundCode&Name.csv", index=False)

    def cleanFundCodesFromCsv(self, fund_file_path):
        log_files = utils.listAllFiles("./log")
        latest_log = ""
        latest_time = 0
        for f in log_files:
            if "1-scrapeData" in f:
                t = os.path.getctime(f)
                if t > latest_time:
                    latest_time = t
                    latest_log = f

        expired_codes = []
        with open(latest_log, "r") as f:
            for l in f.readlines():
                if "共0/0行" in l:
                    expired_codes.append(l.split("[")[-1].split("]")[0])

        fund_code = pd.read_csv(fund_file_path, dtype=str)
        fund_code['expire'] = fund_code.apply(lambda x: 1 if x['fund_code'] in expired_codes else 0, axis=1)
        fund_code = fund_code[fund_code['expire'] == 0].drop(labels=['expire'], axis=1)

        fund_code.to_csv(fund_file_path, index=False)

        logger.info(f'{len(expired_codes)} fund codes expired, {fund_code.shape[0]} left')

    def getFundCodesFromCsv(self, fund_file_path):
        """从csv文件中获取基金代码清单（可从wind或者其他财经网站导出）"""
        fund_code = pd.read_csv(fund_file_path, dtype=str)
        return fund_code.fund_code

    def getFundInfo(self, fund_code, multi=False):
        """获取基金概况基本信息"""
        global count, fund_count
        count = count + 1
        logger.info("Processing [%d/%d] Funds" % (count, fund_count))
        count = 0 if (fund_count == count) else count

        fund_url = 'http://fund.eastmoney.com/f10/jbgk_' + fund_code + '.html'
        res = getURL(fund_url)
        soup = BeautifulSoup(res.text, 'html.parser')
        result = {}

        # parse
        try:
            result['fund_code'] = fund_code
            result['fund_name'] = soup.find_all(text=u"基金全称")[0].next_element.text.strip()
            if result['fund_name'] == "---" or result['fund_name'] == "":
                logger.warning("无效基金代码-[%s] " % fund_code)
                if multi:
                    lock.acquire()
                    outQueue.put(['txt', fund_code])
                    lock.release()
                return result
            result['fund_abbr_name'] = soup.find_all(text=u"基金简称")[0].next_element.text.strip()
            result['fund_type'] = soup.find_all(text=u"基金类型")[0].next_element.text.strip()
            result['issue_date'] = soup.find_all(text=u"发行日期")[0].next_element.text.strip()
            result['establish_date'] = soup.find_all(text=u"成立日期/规模")[0].next_element.text.split(u'/')[0].strip()
            result['establish_scale'] = soup.find_all(text=u"成立日期/规模")[0].next_element.text.split(u'/')[-1].strip()
            result['asset_value'] = soup.find_all(text=u"资产规模")[0].next_element.text.split(u'（')[0].strip()
            if result['asset_value'] != "---":
                result['asset_value_date'] = \
                    soup.find_all(text=u"资产规模")[0].next_element.text.split(u'（')[1].split(u'）')[0].strip(u'截止至：')
            else:
                result['asset_value_date'] = result['asset_value']
            result['units'] = soup.find_all(text=u"份额规模")[0].next_element.text.strip().split(u'（')[0].strip()
            if result['units'] != "---":
                result['units_date'] = soup.find_all(text=u"份额规模")[0].next_element.text.strip().split(u'（')[1].strip(
                    u'（截止至：）')
            else:
                result['units_date'] = result['units']
            result['fund_manager'] = soup.find_all(text=u"基金管理人")[0].next_element.text.strip()
            result['fund_trustee'] = soup.find_all(text=u"基金托管人")[0].next_element.text.strip()
            result['funder'] = soup.find_all(text=u"基金经理人")[0].next_element.text.strip()
            result['total_div'] = soup.find_all(text=u"成立来分红")[0].next_element.text.strip()
            result['mgt_fee'] = soup.find_all(text=u"管理费率")[0].next_element.text.strip()
            result['trust_fee'] = soup.find_all(text=u"托管费率")[0].next_element.text.strip()
            result['sale_fee'] = soup.find_all(text=u"销售服务费率")[0].next_element.text.strip()
            result['buy_fee'] = soup.find_all(text=u"最高认购费率")[0].next_element.text.strip().split(u'（')[0].strip()
            result['buy_fee2'] = soup.find_all(text=u"最高申购费率")[0].next_element.text.strip().split(u'（')[0].strip()
            result['benchmark'] = soup.find_all(text=u"业绩比较基准")[0].next_element.text.strip(u'该基金暂未披露业绩比较基准')
            result['underlying'] = soup.find_all(text=u"跟踪标的")[0].next_element.text.strip(u'该基金无跟踪标的')
        except Exception as e:
            logger.exception(str("爬取失败: [%s] " % fund_code + fund_url + " %s" % e))

        # import
        if multi:
            lock.acquire()
            outQueue.put(['fund_info', result])
            lock.release()
        else:
            try:
                mySQL.insertData('fund_info', result)
                logger.info("新增基金代码-[%s]-%s" % (result['fund_code'], result['fund_name']))
            except Exception as e:
                logger.exception(str("入库失败: [%s] " % fund_code + fund_url + " %s" % e))
        return result

    def getFundManagers(self, fund_code):
        """获取基金经理基本数据 - 基金经理变动一览表/ 基金经理信息基表"""
        global count, fund_count
        count = count + 1
        logger.info("Processing [%d/%d] Fund Managers" % (count, fund_count))
        count = 0 if (fund_count == count) else count

        fund_url = 'http://fund.eastmoney.com/f10/jjjl_' + fund_code + '.html'
        res = getURL(fund_url)
        soup = BeautifulSoup(res.text, 'html.parser')
        tables = soup.find_all("table")
        tab = tables[1]
        result = {}

        # 基金经理变动一览表
        manager = {}
        for tr in tab.findAll('tr'):
            if tr.findAll('td'):
                try:
                    result['fund_code'] = fund_code
                    result['start_date'] = tr.select('td:nth-of-type(1)')[0].getText().strip()
                    result['end_date'] = tr.select('td:nth-of-type(2)')[0].getText().strip()
                    result['fund_managers'] = tr.select('td:nth-of-type(3)')[0].getText().strip()
                    result['term'] = tr.select('td:nth-of-type(4)')[0].getText().strip()
                    result['return_rate'] = tr.select('td:nth-of-type(5)')[0].getText().strip('%') + '%'
                    result['created_date'] = self.getCurrentTime()
                    result['updated_date'] = self.getCurrentTime()
                    result['data_source'] = 'eastmoney'
                except  Exception as e:
                    logger.exception(str("爬取失败(based on fund): [%s] - %s\n%s" % (fund_code, fund_url, e)))

                try:
                    mySQL.insertData('fund_managers_chg', result)
                except Exception as e:
                    logger.exception(str("入库失败(based on fund): [%s] - %s\n%s" % (fund_code, fund_url, e)))

                # 基金经理信息基表
                for a in tr.findAll('a'):
                    if a:
                        try:
                            manager['manager_id'] = a['href'].strip('http://fund.eastmoney.com/manager/.html')
                            manager['url'] = a['href']
                            manager['manager_name'] = a.text
                            manager['created_date'] = self.getCurrentTime()
                            manager['updated_date'] = self.getCurrentTime()
                            manager['data_source'] = 'eastmoney'
                        except Exception as e:
                            logger.exception(str("爬取失败(based on person): [%s] - %s - %s - %s\n%s" % (
                                fund_code, manager['manager_name'], manager['url'], fund_url, e)))
                        try:
                            mySQL.insertData('managers_info', manager)
                        except Exception as e:
                            logger.exception(str("入库失败(based on person): [%s] - %s - %s - %s\n%s" % (
                                fund_code, manager['manager_name'], manager['url'], fund_url, e)))
        return result

    def getFundManagersHistory(self):
        """获取基金经理履历数据 - 基金经理履历数据"""
        manaList = pd.DataFrame(mySQL.selectDistinct('managers_info'))
        manaList = manaList[['manager_id', 'url', 'manager_name']].drop_duplicates()
        mana_count = len(manaList)
        for i in range(mana_count):
            logger.info("Processing [%d/%d] Managers" % (i + 1, mana_count))
            mana_id = manaList.iloc[i, 0]
            mana_url = manaList.iloc[i, 1]
            mana_name = manaList.iloc[i, 2]
            res = getURL(mana_url)
            res.encoding = 'UTF-8'
            soup = BeautifulSoup(res.text, 'html.parser')
            div = "-"
            if len(soup.find_all("div", class_="right jd ")) > 0:
                div = soup.find_all("div", class_="right jd ")[0].text.split('任职起始日期：')[0].split('累计任职时间：')[1].strip()
            tables = soup.find_all("table")
            if len(tables) > 1:
                tab = tables[1]
                for tr in tab.findAll('tr'):
                    manager_his = {}
                    if tr.findAll('td'):
                        try:
                            manager_his['manager_id'] = mana_id
                            manager_his['manager_url'] = mana_url
                            manager_his['manager_name'] = mana_name
                            manager_his['cum_on_duty_term'] = div
                            manager_his['fund_code'] = tr.select('td:nth-of-type(1)')[0].getText().strip()
                            manager_his['fund_name'] = tr.select('td:nth-of-type(2)')[0].getText().strip()
                            manager_his['fund_type'] = tr.select('td:nth-of-type(4)')[0].getText().strip()
                            manager_his['fund_scale'] = tr.select('td:nth-of-type(5)')[0].getText().strip()
                            manager_his['start_date'] = tr.select('td:nth-of-type(6)')[0].getText().strip().split('~')[
                                0].strip()
                            manager_his['end_date'] = tr.select('td:nth-of-type(6)')[0].getText().strip().split('~')[
                                1].strip()
                            manager_his['term'] = tr.select('td:nth-of-type(7)')[0].getText().strip()
                            manager_his['return_rate'] = tr.select('td:nth-of-type(8)')[0].getText().strip('%') + '%'
                            manager_his['updated_date'] = self.getCurrentTime()
                            manager_his['data_source'] = 'eastmoney'
                        except Exception as e:
                            logger.exception(str("爬取失败(based on manager): [%s] - %s\n%s" % (mana_id, mana_url, e)))
                        try:
                            mySQL.insertData('managers_his', manager_his)
                        except Exception as e:
                            logger.exception(str("入库失败(based on manager): [%s] - %s\n%s" % (mana_id, mana_url, e)))
            else:
                logger.info("页面无法打开： " + mana_id + " " + mana_url + " " + mana_name)

    def getFundNav(self, fund_code, update=False, silent=False, multi=False):
        # 获取基金净值数据
        # init
        global count
        count = count + 1
        logger.info(f"Processing [%d/%d] Funds - NAV [%s]" % (count, fund_count, fund_code))

        per_pg = 49
        # http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code=000001&page=1&per=49
        fund_url = f'http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code=%s&page=1&per=%d' % (
            fund_code, per_pg)
        pages = 0
        records = -1

        # get total rec count
        try:
            res = getURL(fund_url)
            records = (res.text.strip('var apidata=').strip('{;}').split(',')[1].strip('records:'))
            pages = (res.text.strip('var apidata=').strip('{;}').split(',')[2].strip('pages:'))
        except  Exception as e:
            logger.exception(f"爬取记录总数失败: [%s] %s \n%s" % (fund_code, fund_url, e))

        # set pages to scrape
        if not update:
            pages = int(pages)
        else:
            if req_rec >= per_pg:
                pages = (req_rec // per_pg) + 1
            else:
                pages = 1
                per_pg = req_rec

        # get fund nav by pages
        i = 0  # 基金总record数
        for pg in range(1, pages + 1):
            fund_nav = 'http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code=%s&page=%d&per=%d' % (
                fund_code, pg, per_pg)
            res = getURL(fund_nav)
            soup = BeautifulSoup(res.text, 'html.parser')
            tables = soup.findAll('table')
            tab = tables[0]

            # 解析表格，逐行逐单元格获取净值数据
            for tr in tab.findAll('tr'):
                result = {}
                result['fund_code'] = fund_code
                # 跳过表头；获取净值、累计净值和日收益率数据 如果列数为7，可以判断为一般基金。当然也可以通过标题或者基金类型参数来判断，待后续优化
                if tr.findAll('td') and len((tr.findAll('td'))) == 7:
                    i = i + 1
                    try:
                        result['date'] = (
                            tr.select('td:nth-of-type(1)')[0].getText().strip().split(u'起始时间')[0].replace('*',
                                                                                                          '').strip())
                        result['nav'] = (tr.select('td:nth-of-type(2)')[0].getText().strip())
                        result['add_nav'] = (tr.select('td:nth-of-type(3)')[0].getText().strip())
                        result['nav_chg_rate'] = (tr.select('td:nth-of-type(4)')[0].getText().strip())
                        result['buy_state'] = (tr.select('td:nth-of-type(5)')[0].getText().strip())
                        result['sell_state'] = tr.select('td:nth-of-type(6)')[0].getText().strip()
                        result['div_record'] = tr.select('td:nth-of-type(7)')[0].getText().strip().strip('\'')
                        result['created_date'] = self.getCurrentTime()
                        result['updated_date'] = self.getCurrentTime()

                        if multi:
                            lock.acquire()
                            outQueue.put(['nav', result])
                            lock.release()
                        else:
                            mySQL.insertData('nav', result)

                        if silent:
                            pass
                        else:
                            logger.info(
                                "[%d] %d/%d %s - %f" % (result['fund_code'], i, records, result['date'], result['nav']))

                    except Exception as e:
                        logger.exception(str("[" + fund_code + "] 获取失败-%d" % i + fund_url + " %s" % e))

                # 如果是货币基金，获取万份收益和7日年化利率
                elif tr.findAll('td') and len((tr.findAll('td'))) == 6:
                    i = i + 1
                    try:
                        result['date'] = (
                            tr.select('td:nth-of-type(1)')[0].getText().strip().split(u'起始时间')[0].replace('*',
                                                                                                          '').strip())
                        result['profit_per_units'] = (tr.select('td:nth-of-type(2)')[0].getText().strip())
                        result['profit_rate'] = (tr.select('td:nth-of-type(3)')[0].getText().strip())
                        result['buy_state'] = (tr.select('td:nth-of-type(4)')[0].getText().strip())
                        result['sell_state'] = (tr.select('td:nth-of-type(5)')[0].getText().strip())
                        result['div_record'] = (tr.select('td:nth-of-type(6)')[0].getText().strip())
                        result['created_date'] = self.getCurrentTime()
                        result['updated_date'] = self.getCurrentTime()

                        if multi:
                            lock.acquire()
                            outQueue.put(['nav_currency', result])
                            lock.release()
                        else:
                            mySQL.insertData('nav_currency', result)

                        if silent:
                            pass
                        else:
                            logger.info("[%d] %d/%d %s - %f" % (
                                result['fund_code'], i, records, result['date'], result['profit_rate']))

                    except Exception as e:
                        logger.exception(str("[" + fund_code + "] 获取失败-%d" % i + fund_url + " %s" % e))
                else:
                    pass
        logger.info(str("[" + fund_code + "] " + '共' + str(i) + '/' + str(records) + '行'))

        # get fund nav quantity
        result = {}
        result['fund_code'] = fund_code
        result['updated_date'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        result['quantity'] = records

        lock.acquire()
        outQueue.put(['nav_quantity', result])
        lock.release()


def randHeader():
    """随机生成User-Agent"""
    head_connection = ['Keep-Alive', 'close']
    head_accept = ['text/html, application/xhtml+xml, */*']
    head_accept_language = ['zh-CN,fr-FR;q=0.5', 'en-US,en;q=0.8,zh-Hans-CN;q=0.5,zh-Hans;q=0.3']
    ua = {
        'Connection': head_connection[0],
        'Accept': head_accept[0],
        'Accept-Language': head_accept_language[1],
        'User-Agent': UserAgent().Chrome
    }
    return ua


def getURL(url, tries_num=3, sleep_time=7, time_out=3, max_retry=5):
    """
        这里重写get函数，主要是为了实现网络中断后自动重连，同时为了兼容各种网站不同的反爬策略及，通过sleep时间和timeout动态调整来测试合适的网络连接参数；
        通过isproxy 来控制是否使用代理，以支持一些在内网办公的同学
        :param url:
        :param tries_num:  重试次数
        :param sleep_time: 休眠时间
        :param time_out: 连接超时参数
        :param max_retry: 最大重试次数，递归时使用
        :return: response9
        """
    sleep_time_p = sleep_time
    time_out_p = time_out
    tries_num_p = tries_num
    isproxy = 0  # 如需要使用代理，改为1，并设置代理IP参数 proxy
    proxy = {"http": "http://110.37.84.147:8080", "https": "http://110.37.84.147:8080"}  # 这里需要替换成可用的代理IP

    try:
        if isproxy == 1:
            res = requests.get(url, headers=randHeader(), timeout=time_out, proxies=proxy)
        else:
            res = requests.get(url, headers=randHeader(), timeout=time_out)
        return res
    except (socket.timeout or urllib3.exceptions.ReadTimeoutError or requests.exceptions.ReadTimeout)as e:
        # 设置重试次数，最大timeout 时间和 最长休眠时间
        sleep_time_p = sleep_time_p + 3
        time_out_p = time_out_p + 3
        tries_num_p = tries_num_p - 1
        if tries_num_p > 0:
            time.sleep(sleep_time_p)
            logger.exception(f'%s - %d tries connection.' % (url, max_retry - tries_num_p))
            return getURL(url, tries_num_p, sleep_time_p, time_out_p, max_retry)
        else:
            logger.exception(f'%s - error after %d tries connection.' % (url, max_retry - tries_num_p))
            return 1


# 检查净值数据数量
def checkNAV():  # TODO
    try:
        start = time.time()
        fundsCheck = mySQL.selectDistinct('nav_quantity')
        sql = f"select distinct fund_code, count(*) from (select distinct * from %s) t group by fund_code"
        df_nav = mySQL.sql(sql % 'nav')  # from DB
        df_cur = mySQL.sql(sql % 'nav_currency')  # from DB
        df_nav = pd.concat([df_nav, df_cur], axis=0)
        fundsCheck = pd.merge(fundsCheck, df_nav, how='left', on=['fund_code'])
        end = time.time()
        logger.info(f"Time used to query NAV quantity from DB: %d " % ((end - start) / 60))

        def divide(x):
            try:
                res = x['count(*)'] / x['quantity']
            except ZeroDivisionError:
                res = 999
            return res

        fundsCheck[['quantity']] = fundsCheck[['quantity']].apply(pd.to_numeric)
        fundsCheck['pct'] = fundsCheck.apply(divide, axis=1)
        fundsCheck['diff'] = fundsCheck.apply(lambda x: x['quantity'] - x['count(*)'], axis=1)
        fundsCheck.to_csv('./data/5-fund_check.csv', index=False)
        return fundsCheck
    except Exception as e:
        logger.exception(str('NAV QUANTITY CHECK FAIL: ' + " %s" % e))
        return None


class baseThread(threading.Thread):
    def __init__(self, func):
        super(baseThread, self).__init__()  # 调用父类的构造函数
        self.func = func  # 传入线程函数逻辑
        self.daemon = True

    def run(self):
        self.func()


def processWorker():  # combine thread(worker)/queue(task)/function(procedure)
    while not inQueue.empty():  # 启动outQueue时队列已经稳定
        item = inQueue.get()  # 获得任务
        f_code = item[0]
        update_flag = item[1]
        retry = item[2]
        try:
            fundSpiders.getFundNav(f_code, update_flag, True, True)  # update = False(全量)/True(增量),silent, multi
            inQueue.task_done()
            logger.info(' ' * 70 + f'[%s] Done!' % f_code)
            time.sleep(thread_sleep)
        except Exception as e:
            if retry > 0:
                logger.exception(f'processWorker FAIL [%s] - retry in queue!' % f_code)
                inQueue.put([f_code, update_flag, (retry - 1)])
            else:
                logger.exception(f'processWorker FAIL [%s] - reached max retry times! \n%s' % (f_code, e))
                with open('./log/getNAVFail_%s.log' % dt.now().strftime('%Y-%m-%d'), 'a') as f:
                    f.write(f_code)
                inQueue.task_done()


def IOWorker():  # combine thread(worker)/queue(task)/function(procedure)
    counter = 0
    tries = 0
    while True:
        if outQueue.qsize():  # 返回队列的大小
            tries = 0
            item = outQueue.get()
            try:
                mySQL.insertData(item[0], item[1])  # 将结果存入数据库中
            except:
                with open('./log/importNAVFail_%s.log' % dt.now().strftime('%Y-%m-%d'), 'a') as f:
                    f.write(item[1]['fund_code'])
            finally:
                outQueue.task_done()
                counter = counter + 1
                if counter % 2000 == 0:
                    logger.info('Importing Data - %d/%d' % (counter, outQueue.qsize()))
        else:
            tries = tries + 1
            if tries < 5:
                time.sleep(10)
                logger.info('IO worker sleep - %d' % tries)
            else:
                break
    logger.info('Finish importing Data - %d' % counter)


def processWorker_simple():  # combine thread(worker)/queue(task)/function(procedure)
    while not inQueue.empty():  # 启动outQueue时队列已经稳定
        item = inQueue.get()  # 获得任务
        f_code = item[0]
        fundSpiders.getFundInfo(f_code, True)  # update = False(全量)/True(增量),silent, multi
        inQueue.task_done()
        time.sleep(thread_sleep)


def IOWorker_simple():  # combine thread(worker)/queue(task)/function(procedure)
    counter = 0
    tries = 0
    while True:
        if outQueue.qsize():  # 返回队列的大小
            tries = 0
            item = outQueue.get()
            if item[0] == 'txt':
                with open("./dep/0-doneFundCode.txt", 'a') as f:
                    f.writelines(item[1])
                    f.writelines('\n')
            else:
                try:
                    mySQL.insertData(item[0], item[1])  # 将结果存入数据库中
                    logger.info("新增基金代码-[%s]-%s" % (item[1]['fund_code'], item[1]['fund_name']))
                except:
                    pass
                finally:
                    outQueue.task_done()
                    counter = counter + 1
                    if counter % 2000 == 0:
                        logger.info('Importing Data - %d/%d' % (counter, outQueue.qsize()))
        else:
            tries = tries + 1
            if tries < 5:
                time.sleep(10)
                logger.info('IO worker sleep - %d' % tries)
            else:
                break
    logger.info('Finish importing Data - %d' % counter)


def welcome():
    logger.info('\nPlz input no. to execute commands:\n'
                '0.Update FUND CODE LIST\n'
                '\n'
                '1.Update all NAV INFO\n'
                '2.Update last XX days NAV INFO\n'
                '\n'
                '3.Update all MANAGER INFO\n'
                '4.Update all MANAGER HISTORY\n'
                '\n'
                '5.Exam fund data quality\n'  # TODO
                '\n'
                '6.Clean all MANAGER TABLE\n'
                '7.Export all TABLES\n'
                '\n'
                '8.Update ONE FUND \n'
                '9.Exit\n'
                '\n'
                'Command NO.: ')
    n = sys.stdin.readline().strip('\n')
    if int(n) in [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]:
        return int(n)
    else:
        logger.info('INVALID INPUT\n')
        welcome()


def main():
    # init database instance
    global mySQL
    mySQL = PyMySQL.PyMySQL()
    mySQL._init_(*db)

    # init threading
    global request_sleep, inQueue, outQueue, lock, thread_finish, thread_sleep
    request_sleep = 0.4
    inQueue = queue.Queue()
    outQueue = queue.Queue()
    lock = threading.Lock()
    thread_finish = False
    thread_sleep = 2

    # init scrape instance
    global fundSpiders, fund_count, count, req_rec
    fundSpiders = FundSpiders()
    fundSpiders.cleanFundCodesFromCsv(fund_file_path)
    funds = fundSpiders.getFundCodesFromCsv(fund_file_path)
    fund_count = len(funds)

    while True:
        # get command
        n = welcome()
        count = 0

        # number of process
        # threadNum = int(input('Number of threads: '))
        threadNum = 30

        if (n == 1 or n == 2):  # 1.Update all NAV INFO/2.Update last 98 days NAV INFO
            update_flag = (False if n == 1 else True)
            req_rec = int(input('Days of records to retrive: '))

            start = time.time()
            # create process queue ->fund_code/ func/ arg/ max retry time
            [inQueue.put([fund, 'NAV', update_flag, 3]) for fund in funds]

            threads = [baseThread(processWorker) for _ in range(threadNum)]  # create process thread
            threads.append(baseThread(IOWorker))  # create IO thread
            [thread.start() for thread in threads[:-1]]  # start process thread
            time.sleep(30)
            threads[-1].start()  # start IO thread
            [thread.join() for thread in threads]  # wait until all process finishes tasks

            logger.info('Time used multithreading scraping NAV: %s' % str(time.time() - start))

        elif n == 3:  # 3.Update all MANAGER INFO
            # funds = funds[::-1]
            [fundSpiders.getFundManagers(fund) for fund in funds]  # manager info, 全量

        elif n == 4:  # 4.Update all MANAGER HISTORY
            fundSpiders.getFundManagersHistory()  # manager history, 全量

        elif n == 5:  # 5.Exam fund data quality
            # exam fund data quality
            fundsCheck = checkNAV()
            if fundsCheck is None:
                pass
            else:
                fundsAbnoraml = fundsCheck[fundsCheck['diff'].abs() >= 1]
                # delete null fund data -> 0 nav records
                fundsDel = fundsAbnoraml[fundsAbnoraml['quantity'] == 0]
                fundsDelList = list(fundsDel.fund_code)
                if len(fundsDelList) >= 1:
                    format_strings = ','.join(['%s'] * len(fundsDelList))
                    _sql = "DELETE FROM %s WHERE fund_code IN (%s)" % ('fund_info', format_strings)
                    nrows = mySQL.sql(_sql, tuple(fundsDelList))
                    logger.info('DELETED %d entries from %s for 0 NAV RECORDS' % (nrows, 'fund_info'))
                # delete abnormal fund data -> mismatch data
                fundsAbnoramlList = list(fundsAbnoraml.fund_code)
                if len(fundsAbnoramlList) >= 1:
                    fundsAbnoramlList = list(fundsAbnoraml.fund_code)
                    format_strings = ','.join(['%s'] * len(fundsAbnoramlList))
                    tabs = ['fund_managers_chg', 'nav', 'nav_currency', 'nav_quantity']
                    for tab in tabs:
                        _sql = "DELETE FROM %s WHERE fund_code IN (%s)" % (tab, format_strings)
                        nrows = mySQL.sql(_sql, tuple(fundsAbnoramlList))
                        logger.info('DELETED %d entries from %s for MISMATCHED/OUTDATED DATA' % (nrows, tab))
                # output fund list to re-launch
                fundsAbnoraml = fundsAbnoraml[fundsAbnoraml['quantity'] != 0]
                fundsAbnoraml.to_csv("./dep/5-fundCode&Name.csv", index=False)
            logger.info('PLZ re-launch and update NAV/MANAGER INFO from ABNORMAL LIST!!')

        elif n == 6:  # 6.Clean all MANAGER TABLE
            tabs = ['managers_info', 'fund_managers_chg', 'managers_his']
            for tab in tabs:
                _sql = 'DELETE FROM fund.%s where updated_date <\'%s\'' % (
                    tab, time.strftime('%Y-%m-%d', time.localtime(time.time())))
                mySQL.sql(_sql)
        elif n == 7:  # 7.Export all TABLES
            # export
            for tab in ['fund_info', 'fund_managers_chg', 'managers_info', 'nav', 'nav_currency', 'managers_his']:
                exportQuery(mySQL, tab)

        elif n == 8:  # 8.Update ONE FUND
            logger.info('\nPlz input FUND_CODE:\n')
            fund = sys.stdin.readline().strip('\n')
            if len(fund) == 6:
                fundSpiders.getFundInfo(fund)  # fund info
                fundSpiders.getFundManagers(fund)  # manager info
                fundSpiders.getFundNav(fund, update=False)  # nav info, update = False(全量)/True(增量)
            else:
                logger.exception('INVALID INPUT\n')

        elif n == 0:  # 0.Update FUND CODE LIST/ MP
            fundSpiders.updateFundCodesFromCsv()

        else:
            logger.info('Bye~')
            break

    # close DB connection
    mySQL.dispose()


if __name__ == "__main__":
    main()
