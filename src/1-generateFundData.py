# -*- coding:utf-8 -*-
'''
原PO: https://blog.csdn.net/yuzhucu/article/details/55261024
功能：抓取东方财富网上基金相关数据
原作：yuzhucu
'''
# from gevent import monkey
# monkey.patch_all()
import logging
import queue
import random
import socket
import sys
import threading
import time

import pandas as pd
# import pymysql
import requests
import urllib3
from bs4 import BeautifulSoup
from tqdm import tqdm
from utils import PyMySQL

logging.basicConfig(level=logging.INFO,
                    filename='../log/1-generateFundData.log',
                    filemode='a',
                    datefmt='%Y/%m/%d %H:%M:%S',
                    format='%(levelname)s %(asctime)s %(funcName)s %(lineno)d %(message)s'
                    )
logger = logging.getLogger('main')
logger.addHandler(logging.StreamHandler())  # 输出到控制台的handler


# 随机生成User-Agent
def randHeader():
    head_connection = ['Keep-Alive', 'close']
    head_accept = ['text/html, application/xhtml+xml, */*']
    head_accept_language = ['zh-CN,fr-FR;q=0.5', 'en-US,en;q=0.8,zh-Hans-CN;q=0.5,zh-Hans;q=0.3']
    head_user_agent = ['Opera/8.0 (Macintosh; PPC Mac OS X; U; en)',
                       'Opera/9.27 (Windows NT 5.2; U; zh-cn)',
                       'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Win64; x64; Trident/4.0)',
                       'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0)',
                       'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.2; .NET4.0C; .NET4.0E)',
                       'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.2; .NET4.0C; .NET4.0E; QQBrowser/7.3.9825.400)',
                       'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0; BIDUBrowser 2.x)',
                       'Mozilla/5.0 (Windows; U; Windows NT 5.1) Gecko/20070309 Firefox/2.0.0.3',
                       'Mozilla/5.0 (Windows; U; Windows NT 5.1) Gecko/20070803 Firefox/1.5.0.12',
                       'Mozilla/5.0 (Windows; U; Windows NT 5.2) Gecko/2008070208 Firefox/3.0.1',
                       'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.12) Gecko/20080219 Firefox/2.0.0.12 Navigator/9.0.0.6',
                       'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1500.95 Safari/537.36',
                       'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; rv:11.0) like Gecko)',
                       'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:21.0) Gecko/20100101 Firefox/21.0 ',
                       'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Maxthon/4.0.6.2000 Chrome/26.0.1410.43 Safari/537.1 ',
                       'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.92 Safari/537.1 LBBROWSER',
                       'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.75 Safari/537.36',
                       'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.11 TaoBrowser/3.0 Safari/536.11',
                       'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko',
                       'Mozilla/5.0 (Macintosh; PPC Mac OS X; U; en) Opera 8.0'
                       ]
    result = {
        'Connection': head_connection[0],
        'Accept': head_accept[0],
        'Accept-Language': head_accept_language[1],
        'User-Agent': head_user_agent[random.randrange(0, len(head_user_agent))]
            }
    return result


# 获取当前时间
def getCurrentTime():
    return time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time()))


# 获取网页
def getURL(url, tries_num=5, sleep_time=5, time_out=10, max_retry=10):
    '''
        这里重写get函数，主要是为了实现网络中断后自动重连，同时为了兼容各种网站不同的反爬策略及，通过sleep时间和timeout动态调整来测试合适的网络连接参数；
        通过isproxy 来控制是否使用代理，以支持一些在内网办公的同学
        :param url:
        :param tries_num:  重试次数
        :param sleep_time: 休眠时间
        :param time_out: 连接超时参数
        :param max_retry: 最大重试次数，仅仅是为了递归使用
        :return: response
        '''
    sleep_time_p = sleep_time
    time_out_p = time_out
    tries_num_p = tries_num
    try:
        res = requests.Session()
        if isproxy == 1:
            res = requests.get(url, headers=header, timeout=time_out, proxies=proxy)
        else:
            res = requests.get(url, headers=header, timeout=time_out)
        # res.raise_for_status()  # 如果响应状态码不是 200，就主动抛出异常
        return res
    except (socket.timeout or urllib3.exceptions.ReadTimeoutError or requests.exceptions.ReadTimeout)as e:  # TODO error msg
        sleep_time_p = sleep_time_p + 5
        time_out_p = time_out_p + 5
        tries_num_p = tries_num_p - 1 # 设置重试次数，最大timeout 时间和 最长休眠时间
        if tries_num_p > 0:
            time.sleep(sleep_time_p)
            logger.exception(str(url + '\nURL Connection Error: 第' + str(max_retry - tries_num_p) + '次 Retry Connection.'))
            return getURL(url, tries_num_p, sleep_time_p, time_out_p, max_retry)
        else:
            logger.exception(
                str(url + '\nURL Connection Error after %d'%max_retry +'retry Connection.'))
            return 1


# class PyMySQL:
#     # 数据库初始化
#     def _init_(self, host, user, passwd, db, port=3306, charset='utf8'):
#         pymysql.install_as_MySQLdb()
#         try:
#             self.conn = pymysql.connect(host=host, user=user, passwd=passwd, db=db, port=3306, charset='utf8')
#             self.conn.ping(reconnect=True)  # 使用mysql ping来检查连接,实现超时自动重新连接
#             self.conn.autocommit(True)
#             self.cur = self.conn.cursor()
#             logger.info("MySQL DB Connect Success: " + user + '@' + host + ':' + str(port) + '/' + db)
#         except  Exception as e:
#             logger.exception(str("MySQL DB Connect Error: %s" % e))
#
#     # 插入数据
#     def insertData(self, table, my_dict,batch = False):
#         try:
#             cols = ', '.join(my_dict.keys())
#             values = '","'.join(my_dict.values())
#             sql = "replace into %s (%s) values (%s)" % (table, cols, '"' + values + '"')
#             result = self.cur.execute(sql) # return rows effected
#             # insert_id = self.conn.insert_id() # 0 if OK
#             if  not result:  # 判断是否执行成功
#                 return 1
#         except Exception as e:
#             self.conn.rollback()  # 发生错误时回滚
#             logger.exception(str("Data Insert Failed: %s" % e))
#             return 1
#
#     # 批量插入数据
#     def executeManyData(self, table, my_dict):
#         try:
#             # TODO batch insert
#             cols = ', '.join(my_dict[0].keys())
#             len_dict = len(my_dict[0])
#             for i in range(len(my_dict)):
#                 my_dict[i] = tuple(my_dict[i])
#             sql = "replace into %s (%s) values (" % (table, cols) + '%s,' * (len_dict - 1) + '%s)'
#             result = self.cur.executemany(sql, my_dict)  # return rows effected
#             insert_id = self.conn.insert_id()  # 0 if OK
#             if result:  # 判断是否执行成功
#                 return insert_id
#             else:
#                 return 1
#         except Exception as e:
#             self.conn.rollback()  # 发生错误时回滚
#             logger.exception(str("Data Insert Failed: %s" % e))
#             return 1
#
#     # 查询数据
#     def queryData(self, table):
#         try:
#             sql = 'select DISTINCT * from %s' % table
#             self.cur.execute(sql)
#             data = self.cur.fetchall()
#             data_dict = []
#             for field in self.cur.description:
#                 data_dict.append(field[0])
#             frame = pd.DataFrame(list(data), columns=data_dict)
#             logger.info(str("Successfully retrieved " + str(self.cur.execute(sql)) + " records from %s" % table))
#             return frame
#         except Exception as e:
#             logger.exception(str("Fail to retrieve data from %s" % table))
#             return 1
#
#     # 建表
#     def createTable(self, table,sourceTab,mode = 'like'):
#         if mode == 'like':
#             sql = "create table %s like %s" % (table,sourceTab)
#             logger.info('Executing %s' % sql)
#             try:
#                 result = self.cur.execute(sql)
#                 self.conn.commit()
#                 # 判断是否执行成功
#                 if result:
#                     msg = str("Table %s Create Succeed!" % table)
#                     logger.info(msg)
#             except Exception as e:
#                 # 发生错误时回滚
#                 self.conn.rollback()
#                 msg = str("Table %s Create Failed: %s" % (table, e))
#                 logger.exception(msg)
#         else:
#             logger.exception('More create table mode TBC~')
#
#     # 删除重复表数据| DISCARD
#     def distinctTable(self, table):
#         # for nav only
#         newTableName = table + '_unique'
#         sqlAssureNew = "drop table if exists %s" % newTableName
#         sql = "create table %s as select distinct * from %s" % (newTableName, table)
#         try:
#             AN = self.cur.execute(sqlAssureNew)
#             if AN:
#                 result = self.cur.execute(sql)
#                 self.conn.commit()
#                 # 判断是否执行成功
#                 if result:
#                     msg = str("Table %s Update Succeed!" % table)
#                     logger.info(msg)
#         except Exception as e:
#             # 发生错误时回滚
#             self.conn.rollback()
#             msg = str("Table %s Update Failed: %s" % (table, e))
#             logger.exception(msg)
#
#     # 清空表数据| DISCARD
#     def truncateTable(self, table):
#         sql = "truncate table %s" % table
#         logger.info('Executing %s' % sql)
#         try:
#             result = self.cur.execute(sql)
#             self.conn.commit()
#             # 判断是否执行成功
#             if result:
#                 msg = str("Table %s Truncate Succeed!" % table)
#                 logger.info(msg)
#         except Exception as e:
#             # 发生错误时回滚
#             self.conn.rollback()
#             msg = str("Table %s Truncate Failed: %s" % (table, e))
#             logger.exception(msg)
#
#     # 删表
#     def dropTable(self, table):
#         cfm = bool(input('Plz confirm to DROP table %s :'%table))
#         if cfm :
#             sql = "drop table if exists %s" % table
#             logger.info('Executing %s' % sql)
#             try:
#                 result = self.cur.execute(sql)
#                 self.conn.commit()
#                 # 判断是否执行成功
#                 if result:
#                     msg = str("Table %s Drop Succeed!" % table)
#                     logger.info(msg)
#             except Exception as e:
#                 # 发生错误时回滚
#                 self.conn.rollback()
#                 msg = str("Table %s Drop Failed: %s" % (table, e))
#                 logger.exception(msg)
#         else:
#             logger.exception('STOP by user.')
#
#     # 查询库中NAV数据量
#     def queryNAVQuantity(self, table):
#         try:
#             sql = "select distinct fund_code, count(*) from (select distinct * from %s) t group by fund_code" % table
#             self.cur.execute(sql)
#             data = self.cur.fetchall()
#             frame = pd.DataFrame(list(data),columns = ['fund_code', 'quantityFromDB'])
#             logger.info(str("Successfully query NAV Quantity " + str(self.cur.execute(sql)) + " records from %s" % table))
#             return frame
#         except Exception as e:
#             logger.exception(str("Fail to query NAV Quantity from %s" % table))
#             return 1
#
#     # 迁移库表
#     def migrateTable(self, dest,source):
#         try:
#             sql = "replace into %s select * from %s" % (dest,source)
#             result = self.cur.execute(sql)
#             insert_id = self.conn.insert_id()
#             self.conn.commit()
#             # 判断是否执行成功
#             if result:
#                 return insert_id
#             else:
#                 return 1
#         except Exception as e:
#             # 发生错误时回滚
#             self.conn.rollback()
#             logger.exception(str("Table Migrate Failed: %s" % e))
#             return 1
#
#     #释放资源
#     def dispose(self):
#         self.conn.close()
#         self.cur.close()


class FundSpiders():
    def getCurrentTime(self):  # 获取当前时间
        return time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time()))

    def getFundCodesFromCsv(self):   # 从csv文件中获取基金代码清单（可从wind或者其他财经网站导出）
        file_path = "../dep/TMP.csv"
        # file_path = "../dep/1-fundCode_master.csv"
        fund_code = pd.read_csv(file_path, dtype=str)
        Code = fund_code.fund_code
        return Code

    # 获取基金概况基本信息
    def getFundInfo(self, fund_code, mode='default'):
        global count
        count = count + 1
        logger.info("Processing [%d/%d] Funds" % (count, fund_count))

        fund_url = 'http://fund.eastmoney.com/f10/jbgk_' + fund_code + '.html'
        res = getURL(fund_url)
        soup = BeautifulSoup(res.text, 'html.parser')
        result = {}
        try:
            # 之前用select、find 比较多，但是一些网页中经常出现部分字段不全导致内容和数据库不匹配的情况导致数据错位。
            # 这里改为用使用标题的next_element 来获取数据值来规避此问题。其中也有个别字段有问题的，特殊处理下即可
            result['fund_code'] = fund_code
            result['fund_name'] = soup.find_all(text=u"基金全称")[0].next_element.text.strip()
            result['fund_abbr_name'] = soup.find_all(text=u"基金简称")[0].next_element.text.strip()
            # result['fund_code']= soup.find_all(text=u"基金代码")[0].next_element )
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
        except  Exception as e:
            logger.exception(str("爬取失败: [" + fund_code + "] " + fund_url + " %s" % e))
        try:
            mySQL.insertData('fund_info', result)
            msg = str(result['fund_code'] + " " + result['fund_name'] + " " + result['fund_abbr_name'] + " " + result[
                'fund_manager'] + " " + result['funder'] + " " + result['establish_date'])
            logger.info(msg)
        except  Exception as e:
            msg = str("入库失败: [" + fund_code + "] " + fund_url + " %s" % e)
            logger.exception(msg)
        return result

    # 获取基金经理基本数据 - 基金经理变动一览表/ 基金经理信息基表
    def getFundManagers(self, fund_code):
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
                # i = i + 1
                try:
                    result['fund_code'] = fund_code
                    result['start_date'] = tr.select('td:nth-of-type(1)')[0].getText().strip()
                    result['end_date'] = tr.select('td:nth-of-type(2)')[0].getText().strip()
                    result['fund_managers'] = tr.select('td:nth-of-type(3)')[0].getText().strip()
                    result['term'] = tr.select('td:nth-of-type(4)')[0].getText().strip()
                    result['return_rate'] = tr.select('td:nth-of-type(5)')[0].getText().strip('%') + '%'
                    result['created_date'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                    result['updated_date'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                    result['data_source'] = 'eastmoney'
                except  Exception as e:
                    msg = str("爬取失败(based on fund): [" + fund_code + "] " + fund_url + " %s" % e)
                    logger.exception(msg)
                try:
                    mySQL.insertData('fund_managers_chg', result)
                    msg = str(
                        result['fund_code'] + " " + result['start_date'] + " " + result['end_date'] + " " + result['fund_managers'] + " " + result['term'] + " " + result['return_rate'])
                    logger.info(msg)
                except  Exception as e:
                    msg = str("入库失败(based on fund): [" + fund_code + "] " + fund_url + " %s" % e)
                    logger.exception(msg)
                # 基金经理信息基表
                for a in tr.findAll('a'):
                    if a:
                        try:
                            manager['manager_id'] = a['href'].strip('http://fund.eastmoney.com/manager/.html')
                            manager['url'] = a['href']
                            manager['manager_name'] = a.text
                            manager['created_date'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                            manager['updated_date'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                            manager['data_source'] = 'eastmoney'
                        except Exception as e:
                            msg = str("爬取失败(based on persons): [" + fund_code + "] " + manager['manager_name'] + " " +manager['url'] + " " + fund_url + " %s" % e)
                            logger.exception(msg)
                        try:
                            mySQL.insertData('fund_managers_info', manager)
                            msg = str(fund_code + " " + manager['manager_name'] + " " + manager['url'] + " " + manager['manager_id'])
                            logger.info(msg)
                        except Exception as e:
                            msg = str("入库失败(based on persons): [" + fund_code + "] " + manager['manager_name'] + " " +manager['url'] + " " + fund_url + " %s" % e)
                            logger.exception(msg)
        return result

    # 获取基金经理履历数据 - 基金经理履历数据
    def getFundManagersHistory(self):
        manaList = pd.DataFrame(mySQL.queryData('fund_managers_info'))
        manaList = manaList[['manager_id','url','manager_name']].drop_duplicates()
        mana_count = len(manaList)
        for i in range(mana_count):
            logger.info("\nProcessing [%d/%d] Managers" % (i+1, mana_count))
            mana_id = manaList.iloc[i, 0]
            mana_url = manaList.iloc[i,1]
            mana_name = manaList.iloc[i,2]
            res = getURL(mana_url)
            res.encoding = 'UTF-8'
            soup = BeautifulSoup(res.text, 'html.parser')
            div ="-"
            if len(soup.find_all("div",class_="right jd "))>0:
                div = soup.find_all("div",class_="right jd ")[0].text.split('任职起始日期：')[0].split('累计任职时间：')[1].strip()
            tables = soup.find_all("table")
            if len(tables)>1:
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
                            manager_his['start_date'] = tr.select('td:nth-of-type(6)')[0].getText().strip().split('~')[0].strip()
                            manager_his['end_date'] = tr.select('td:nth-of-type(6)')[0].getText().strip().split('~')[1].strip()
                            manager_his['term'] = tr.select('td:nth-of-type(7)')[0].getText().strip()
                            manager_his['return_rate'] = tr.select('td:nth-of-type(8)')[0].getText().strip('%') + '%'
                            manager_his['updated_date'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                            manager_his['data_source'] = 'eastmoney'
                        except Exception as e:
                            logger.exception(str("爬取失败(based on manager): [" + mana_id + "] " + mana_url + " %s" % e))
                        try:
                            mySQL.insertData('fund_managers_his', manager_his)
                            logger.info(str(manager_his['manager_id'] + " " + manager_his['manager_url'] + " " + manager_his['manager_name']+ " " + manager_his['fund_code'] ))
                        except Exception as e:
                            logger.exception(str("入库失败(based on manager): [" + mana_id + "] " + mana_url +" %s" % e))
            else:
                logger.info("页面无法打开： "+mana_id + " " + mana_url + " " + mana_name)

    # 获取基金净值数据
    def getFundNav(self, fund_code, update=False, silent = False, multi= False):
        global count
        count = count + 1
        logger.info("Processing [%d/%d] Funds - NAV ["% (count, fund_count) + fund_code + "]" )

        fund_url = 'http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code=' + fund_code + '&page=1&per=49' # http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code=000001&page=1&per=49
        pages = 0
        records = 0

        # 获取历史净值的总记录数与页数
        try:
            '''
            获取单个基金的第一页数据，里面返回的apidata 接口中包含了记录数、分页及数据文件等 
            这里暂按照字符串解析方式获取，既然是标准API接口，应该可以通过更高效的方式批量获取全部净值数据，待后续研究。
            首次初始化完成后，如果后续每天更新或者定期更新，只要修改下每页返回的记录参数即可 
            '''
            res = getURL(fund_url)
            records = (res.text.strip('var apidata=').strip('{;}').split(',')[1].strip('records:'))
            pages = (res.text.strip('var apidata=').strip('{;}').split(',')[2].strip('pages:'))
        except  Exception as e:
            logger.exception(str("爬取记录总数失败: [" + fund_code + "] " + fund_url + " %s" % e))

        # 如增量更新，取60个交易日数据
        if not update:
            pages = int(pages)
        else:
            pages = 1

        # 根据基金代码和总记录数，分页返回所有历史净值
        i = 0  # 基金总record数
        for pg in range(1, pages+1):
            fund_nav = 'http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code=' + fund_code + '&page=' + str(pg) + '&per=49'
            res = getURL(fund_nav)
            soup = BeautifulSoup(res.text, 'html.parser')

            tables = soup.findAll('table')
            tab = tables[0]
            tab_name =''
            # 解析表格，逐行逐单元格获取净值数据
            for tr in tab.findAll('tr'):
                result = {} # WTF 20190120
                result['fund_code'] = fund_code
                # 跳过表头；获取净值、累计净值和日收益率数据 如果列数为7，可以判断为一般基金。当然也可以通过标题或者基金类型参数来判断，待后续优化
                if tr.findAll('td') and len((tr.findAll('td'))) == 7:
                    i = i + 1
                    try:
                        result['the_date'] = (tr.select('td:nth-of-type(1)')[0].getText().strip().split(u'起始时间')[0].replace('*','').strip())
                        result['nav'] = (tr.select('td:nth-of-type(2)')[0].getText().strip())
                        result['add_nav'] = (tr.select('td:nth-of-type(3)')[0].getText().strip())
                        result['nav_chg_rate'] = (tr.select('td:nth-of-type(4)')[0].getText().strip())
                        result['buy_state'] = (tr.select('td:nth-of-type(5)')[0].getText().strip())
                        result['sell_state'] = tr.select('td:nth-of-type(6)')[0].getText().strip()
                        result['div_record'] = tr.select('td:nth-of-type(7)')[0].getText().strip().strip('\'')
                        if multi:
                            lock.acquire()
                            outQueue.put(['fund_nav_slave', result])
                            lock.release()
                        else:
                            mySQL.insertData('fund_nav_slave', result)
                        if silent:
                            pass
                        else:
                            logger.info(str("[" + result['fund_code'] + "] " + str(i) + '/' + str(records) + " " + result['the_date'] + " " + result['nav']))
                    except Exception as e:
                        logger.exception(str("[" + fund_code + "] 获取失败-%d"%i + fund_url + " %s" % e))
                # 如果是货币基金，获取万份收益和7日年化利率
                elif tr.findAll('td') and len((tr.findAll('td'))) == 6:
                    i = i + 1
                    try:
                        result['the_date'] = (tr.select('td:nth-of-type(1)')[0].getText().strip().split(u'起始时间')[0].replace('*','').strip())
                        result['profit_per_units'] = (tr.select('td:nth-of-type(2)')[0].getText().strip())
                        result['profit_rate'] = (tr.select('td:nth-of-type(3)')[0].getText().strip())
                        result['buy_state'] = (tr.select('td:nth-of-type(4)')[0].getText().strip())
                        result['sell_state'] = (tr.select('td:nth-of-type(5)')[0].getText().strip())
                        result['div_record'] = (tr.select('td:nth-of-type(6)')[0].getText().strip())
                        if multi:
                            lock.acquire()
                            outQueue.put(['fund_nav_currency_slave', result])
                            lock.release()
                        else:
                            mySQL.insertData('fund_nav_currency_slave', result)
                        if silent:
                            pass
                        else:
                            logger.info(str("[" + result['fund_code'] + "] " + str(i) + '/' + str(records) + " " + result['the_date'] + " " + result['profit_rate']))
                    except  Exception as e:
                        logger.exception(str("[" + fund_code + "] 获取失败-%d" % i + fund_url + " %s" % e))
                else:
                    pass
        logger.info(str("[" + fund_code + "] " + '共' + str(i) + '/' + str(records) + '行'))

    # 获取历史净值的总记录数(net) - multiply thread mode only
    def getNavQuan(self, fund_code):
        global count
        count = count + 1
        logger.info("Processing [%d/%d] Funds - NAV quantity ["% (count, fund_count) + fund_code + "]" )

        fund_url = 'http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code=' + fund_code + '&page=1&per=1' # http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code=000001&page=1&per=1
        result ={}
        result['fund_code'] = fund_code
        result['updated_date'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

        try:
            res = getURL(fund_url)
            result['quantity'] = (res.text.strip('var apidata=').strip('{;}').split(',')[1].strip('records:'))
        except  Exception:
            logger.exception(str("爬取记录总数失败: [" + fund_code + "] " + fund_url))
            result['quantity'] = -1
        outQueue.put(result)

# 检查净值数据数量
def checkNAV(internet= True):
    try:
        if internet:
            start = time.time()
            pbar = tqdm(total=outQueue.qsize(),dynamic_ncols=True)
            while not outQueue.empty():  # 启动outQueue时队列已经稳定
                result = outQueue.get()
                pbar.update(1)
                if (result['quantity'] !=-1) and (len(result['quantity'])<15):
                    mySQL.insertData('fund_nav_quantity', result)
            pbar.close()
            end = time.time()
            logger.info("Time used to archive NAV quantity from Internet: " + str(end - start))

        end = time.time()
        fundsCheck = mySQL.queryData('fund_nav_quantity')
        df_nav = mySQL.queryNAVQuantity('fund_nav')  # from DB
        df_cur = mySQL.queryNAVQuantity('fund_nav_currency')  # from DB
        df_nav = pd.concat([df_nav, df_cur], axis=0)
        fundsCheck = pd.merge(fundsCheck, df_nav, how='left', on=['fund_code'])
        start = time.time()
        logger.info("Time used to query NAV quantity from DB: " + str(start - end))

        fundsCheck['pct'] = fundsCheck.apply(
            lambda x: pd.to_numeric(x['quantityFromDB'], errors='coerce') / pd.to_numeric(x['quantity'],errors='coerce'), axis=1)
        fundsCheck['diff'] = fundsCheck.apply(
            lambda x: pd.to_numeric(x['quantity'], errors='coerce') - pd.to_numeric(x['quantityFromDB'],errors='coerce'), axis=1)
        fundsCheck.to_csv('../out/1-fundsCheck_%s.csv'%str(time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))), index=False)
    except Exception as e:
        logger.exception(str('NAV QUANTITY CHECK FAIL: ' + " %s" % e))


# 导出表格为csv
def exportTable(tab):
    try:
        pd.DataFrame(mySQL.queryData(tab)).to_csv('../out/1-%s.csv' % tab,index=False)
        msg = str('EXPORT TABLES %s Succeed!' % tab)
        logger.info(msg)
    except Exception as e:
        msg = str('EXPORT TABLES %s Fail:\n%s' % (tab, e))
        logger.exception(msg)


class baseThread(threading.Thread):
    def __init__(self, func):
        super(baseThread, self).__init__()  # 调用父类的构造函数
        self.func = func  # 传入线程函数逻辑
        self.daemon = True

    def run(self):
        self.func()

def processThread():  # combine thread(worker) /queue(task) / function(procedure)
    while not inQueue.empty():  # 启动outQueue时队列已经稳定
        item = inQueue.get()  #获得任务
        try:
            if item[1] =='NAV':
                fundSpiders.getFundNav(item[0], item[2],True,True)  # fund_code, update = False(全量)/True(增量),silent_flag, multi_flag
            elif item[1] =='QUANTITY':
                fundSpiders.getNavQuan(item[0])
            else:
                logger.exception('Processing function NOT specifified !')
                pass
            inQueue.task_done()
            logger.info(' ' *70+'[' + item[0] + '] Done!')
            time.sleep(2)
        except Exception as e:
            logger.exception(str('processThread FAIL [' + item[0] + ']\n' + " %s" % e))
            inQueue.task_done()  # TODO retry logic

def IOThread():
    while (not outQueue.empty()) or (not process_finish):  # 启动时outQueue 还在put
        if not process_finish:
            item = outQueue.get()
            try:
                mySQL.insertData(item[0], item[1])  # 将结果存入数据库中
            finally:
                outQueue.task_done()
        else:
            logger.info('%d Total Left to I/O...' % int(outQueue.qsize()))
            pbar = tqdm(total = outQueue.qsize(),ncols= 500)
            while outQueue.qsize():  # 返回队列的大小
                item = outQueue.get()
                try:
                    mySQL.insertData(item[0], item[1])  # 将结果存入数据库中
                finally:
                    pbar.update(1)
                    outQueue.task_done()
            pbar.close()
            break

# 功能选择对话
def welcome():
    logger.info('\nPlz input no. to execute commands:\n'
                '1.Update all FUND INFO & MANAGER INFO(& export)\n'
                '2.Update all NAV INFO(& export)\n'
                '3.Update last 49 days NAV INFO(& export)\n'
                '4.Update all MANAGER HISTORY(& export)\n'
                '5.Just Check NAV INFO\n'
                '6.Export all TABLES\n'
                '7.Update ONE FUND INFO & MANAGER INFO(& export)\n'
                '8.Update ONE NAV INFO(& export)\n'
                '9.Update ONE MANAGER HISTORY(& export)\n'
                '0.Exit\n'
                '\n'
                'Command NO.: ')
    n = sys.stdin.readline().strip('\n')
    if int(n) in [0,1, 2, 3, 4, 5, 6,7, 8,9]:
        return int(n)
    else:
        logger.info('INVALID INPUT\n')
        welcome()


# MAIN
def main():
    global mySQL,fundSpiders, sleep_time, isproxy, proxy, header, inQueue,outQueue, lock,process_finish, fund_count, count
    mySQL = PyMySQL.PyMySQL()
    mySQL._init_('localhost', 'root', 'JONC', 'fund')  # host/user/password/database

    isproxy = 0  # 如需要使用代理，改为1，并设置代理IP参数 proxy
    proxy = {"http": "http://110.37.84.147:8080", "https": "http://110.37.84.147:8080"}  # 这里需要替换成可用的代理IP
    header = randHeader()
    sleep_time = 0.2
    fundSpiders = FundSpiders()

    inQueue = queue.Queue()
    outQueue = queue.Queue()
    lock = threading.Lock()
    process_finish = False

    funds = fundSpiders.getFundCodesFromCsv()
    fund_count = len(funds)

    while True:
        n = welcome()
        count = 0
        multiProcessing = int(input('number of process: '))
        # multiProcessing = 100

        if n == 1:  # 1.Update all FUND INFO & MANAGER INFO(& export)
            # scape & import
            [fundSpiders.getFundInfo(fund) for fund in funds]  # fund info 全量
            [fundSpiders.getFundManagers(fund) for fund in funds]  # manager info, 全量
            # export
            [exportTable(tab) for tab in ['fund_info', 'fund_managers_info', 'fund_managers_chg']]
        elif (n == 2 or n == 3):  # 2.Update all NAV INFO(& export)/3.Update last 60 days NAV INFO(& export)
            flag = (False if n == 2 else True)
            if multiProcessing <=1:
                # scape & import
                [fundSpiders.getFundNav(fund, update=flag) for fund in funds]  # nav info, update = False(全量)/True(增量)
            else:  # multiple thread
                start = time.time()
                [inQueue.put([fund, 'NAV', flag]) for fund in funds]  # create process queue ->fund_code/ func/ arg
                threads = [baseThread(processThread) for _ in range(multiProcessing)]  # create process thread
                threads.append(baseThread(IOThread)) # create IO thread

                [thread.start() for thread in threads]  # start thread

                [thread.join() for thread in threads[:-1]]  # wait until all process finishes tasks
                inQueue.join() # wait until all tasks to be done
                process_finish = True

                outQueue.join() # wait until all data to be transferred
                threads[-1].join()
                logger.info('Time used multithreading scraping NAV: %s' % str(time.time() - start))
            # migrate
            mySQL.migrateTable('fund_nav','fund_nav_slave')
            mySQL.migrateTable('fund_nav_currency', 'fund_nav_currency_slave')
            # export
            [exportTable(tab) for tab in ['fund_nav', 'fund_nav_currency']]
        elif n == 4:  # 4.Update all MANAGER HISTORY(& export)
            # scape & import
            fundSpiders.getFundManagersHistory()  # manager history, 全量
            # export
            exportTable('fund_managers_his')
        elif n == 5: # 5.Just Check NAV INFO
            # from internet
            start = time.time()
            [inQueue.put([fund, 'QUANTITY', None]) for fund in funds]  # build process queue ->fund_code/ func/ arg
            threads = [baseThread(processThread) for _ in range(multiProcessing)]  # create process thread
            [thread.start() for thread in threads]  # start thread
            [thread.join() for thread in threads]  # wait until all process finishes tasks
            inQueue.join()
            logger.info('Time used to multithreading scraping NAV quantity: %s ' % str(time.time() - start))
            # to database
            checkNAV()
        elif n == 6:  # 6.Export all TABLES
            # export
            for tab in ['fund_info', 'fund_managers_info', 'fund_managers_chg', 'fund_nav', 'fund_nav_currency','fund_managers_his']:
                exportTable(tab)
        elif n == 7:  # 7.Update ONE FUND INFO & MANAGER INFO(& export)
            logger.info('\nPlz input FUND_CODE:\n')
            fund = sys.stdin.readline().strip('\n')
            if len(fund) ==6:
                fundSpiders.getFundInfo(fund)  # fund info
                fundSpiders.getFundManagers(fund)  # manager info
                # export
                [exportTable(tab) for tab in ['fund_info', 'fund_managers_info', 'fund_managers_chg']]
            else:
                logger.exception('INVALID INPUT\n')
        elif n == 8:  # 8.Update ONE NAV INFO(& export)
            logger.info('\nPlz input FUND_CODE:\n')
            fund = sys.stdin.readline().strip('\n')
            if len(fund) ==6:
                fundSpiders.getFundNav(fund, update= False)  # nav info, update = False(全量)/True(增量)
                # export
                [exportTable(tab) for tab in ['fund_nav', 'fund_nav_currency']]
            else:
                logger.exception('INVALID INPUT\n')
        elif n == 9:
            print('TBC')
        else:
            logger.info('Bye~')
            break

    ### close DB connection
    mySQL.dispose()


if __name__ == "__main__":
    main()
