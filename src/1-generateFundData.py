# -*- coding:utf-8 -*-
'''
# 程序：东方财富网基金数据爬取 
# 功能：抓取东方财富网上基金相关数据 
# 创建时间：2017/02/14 基金概况数据 
# 更新历史：2017/02/15 增加基金净值数据 
# 使用库：requests、BeautifulSoup4、pymysql,pandas 
# 作者：yuzhucu 
'''
import logging
import random
import sys
import time

import pandas as pd
import pymysql
import requests
from bs4 import BeautifulSoup

# import os
# import re

logging.basicConfig(level=logging.INFO,
                    filename='../log/1-generateFundData.log',
                    filemode='a',
                    datefmt='%Y/%m/%d %H:%M:%S',
                    format='%(levelname)s %(asctime)s %(funcName)s %(lineno)d %(message)s'
                    )
logger = logging.getLogger(__name__)
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


def getURL(url, tries_num=5, sleep_time=0, time_out=10, max_retry=5):
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
        res.raise_for_status()  # 如果响应状态码不是 200，就主动抛出异常
        return res
    except requests.RequestException as e:
        sleep_time_p = sleep_time_p + 10
        time_out_p = time_out_p + 10
        tries_num_p = tries_num_p - 1
        # 设置重试次数，最大timeout 时间和 最长休眠时间
        if tries_num_p > 0:
            time.sleep(sleep_time_p)
            msg = str(url + 'URL Connection Error: 第' + str(max_retry - tries_num_p) + '次 Retry Connection. %s' % e)
            logger.exception(msg)
            return getURL(url, tries_num_p, sleep_time_p, time_out_p, max_retry)


class PyMySQL:
    # 获取当前时间
    def getCurrentTime(self):
        return time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time()))

    # 数据库初始化
    def _init_(self, host, user, passwd, db, port=3306, charset='utf8'):
        pymysql.install_as_MySQLdb()
        try:
            self.db = pymysql.connect(host=host, user=user, passwd=passwd, db=db, port=3306, charset='utf8')
            self.db.ping(True)  # 使用mysql ping来检查连接,实现超时自动重新连接
            msg = "MySQL DB Connect Success: " + user + '@' + host + ':' + str(port) + '/' + db

            logger.info(msg)
            self.cur = self.db.cursor()
        except  Exception as e:
            msg = str("MySQL DB Connect Error: %s" % e)
            logger.exception(msg)

    # 插入数据
    def insertData(self, table, my_dict):
        try:
            cols = ', '.join(my_dict.keys())
            values = '","'.join(my_dict.values())
            sql = "replace into %s (%s) values (%s)" % (table, cols, '"' + values + '"')
            try:
                result = self.cur.execute(sql)
                insert_id = self.db.insert_id()
                self.db.commit()
                # 判断是否执行成功
                if result:
                    # print (self.getCurrentTime(), u"Data Insert Sucess")
                    return insert_id
                else:
                    return 1
            except Exception as e:
                # 发生错误时回滚
                self.db.rollback()
                msg = str("Data Insert Failed: %s" % e)
                logger.exception(msg)
                return 1
        except Exception as e:
            msg = str("MySQLdb Error: %s" % e)
            logger.exception(msg)
            return 1

    # 查询数据
    def queryData(self, table):
        try:
            sql = 'select DISTINCT * from %s' % table
            self.cur.execute(sql)
            data = self.cur.fetchall()
            data_dict = []
            for field in self.cur.description:
                data_dict.append(field[0])
            frame = pd.DataFrame(list(data), columns=data_dict)
            msg = str("Successfully retrieved " + str(self.cur.execute(sql)) + " records from %s" % table)
            logger.info(msg)
            return frame
        except Exception as e:
            msg = str("Fail to retrieve data from %s" % table)
            logger.exception(msg)
            return 1

    # 删除重复表数据 |定期手动？
    def distinctTable(self, table):
        # for nav only
        newTableName = table + '_unique'
        sqlAssureNew = "drop table if exists %s" % newTableName
        sql = "create table %s as select distinct * from %s" % (newTableName, table)
        try:
            AN = self.cur.execute(sqlAssureNew)
            if AN:
                result = self.cur.execute(sql)
                self.db.commit()
                # 判断是否执行成功
                if result:
                    msg = str("Table %s Update Succeed!" % table)
                    logger.info(msg)
        except Exception as e:
            # 发生错误时回滚
            self.db.rollback()
            msg = str("Table %s Update Failed: %s" % (table, e))
            logger.exception(msg)

    # 清空表数据
    def truncateTable(self, table):
        # for fund/manager only
        sql = "truncate table %s" % table
        logger.info('Executing %s' % sql)
        try:
            result = self.cur.execute(sql)
            self.db.commit()
            # 判断是否执行成功
            if result:
                msg = str("Table %s Truncate Succeed!" % table)
                logger.info(msg)
        except Exception as e:
            # 发生错误时回滚
            self.db.rollback()
            msg = str("Table %s Truncate Failed: %s" % (table, e))
            logger.exception(msg)

    # 查询库中NAV数据量
    def queryNAVQuantity(self, table):
        try:
            sql = "select distinct fund_code, count(*) from (select distinct * from %s) t group by fund_code" % table
            self.cur.execute(sql)
            data = self.cur.fetchall()
            frame = pd.DataFrame(list(data))
            msg = str("Successfully query NAV Quantity " + str(self.cur.execute(sql)) + " records from %s" % table)
            logger.info(msg)
            return frame
        except Exception as e:
            msg = str("Fail to query NAV Quantity from %s" % table)
            logger.exception(msg)
            return 1


class FundSpiders():
    # 获取当前时间
    def getCurrentTime(self):
        return time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time()))

    # 从csv文件中获取基金代码清单（可从wind或者其他财经网站导出）
    def getFundCodesFromCsv(self):
        # file_path=os.path.join(os.getcwd(),'fundCode.csv')
        file_path = "../dep/1-fundCode.csv"
        # fund_code = pd.read_csv(file_path,encoding='gbk')
        fund_code = pd.read_csv(file_path, dtype=str)
        Code = fund_code.fund_code
        return Code

    # 获取基金概况基本信息
    def getFundInfo(self, fund_code, mode='default'):
        fund_url = 'http://fund.eastmoney.com/f10/jbgk_' + fund_code + '.html'
        res = getURL(fund_url)
        soup = BeautifulSoup(res.text, 'html.parser')
        result = {}
        try:
            ''' 
           之前用select、find 比较多，但是一些网页中经常出现部分字段不全导致内容和数据库不匹配的情况导致数据错位。
           这里改为用使用标题的next_element 来获取数据值来规避此问题。其中也有个别字段有问题的，特殊处理下即可 
            '''
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
            msg = str("爬取失败: [" + fund_code + "] " + fund_url + " %s" % e)
            logger.exception(msg)
        try:
            mySQL.insertData('fund_info', result)
            msg = str(result['fund_code'] + " " + result['fund_name'] + " " + result['fund_abbr_name'] + " " + result[
                'fund_manager'] + " " + result['funder'] + " " + result['establish_date'])
            logger.info(msg)
        except  Exception as e:
            msg = str("入库失败: [" + fund_code + "] " + fund_url + " %s" % e)
            logger.exception(msg)
        return result

    # 获取基金经理基本数据(基金投资分析关键在投资经理，后续在完善)
    def getFundManagers(self, fund_code):
        fund_url = 'http://fund.eastmoney.com/f10/jjjl_' + fund_code + '.html'
        res = getURL(fund_url)
        soup = BeautifulSoup(res.text, 'html.parser')
        tables = soup.find_all("table")
        tab = tables[1]
        # 基金经理变动一览表
        result = {}
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

    # 获取基金经理履历数据
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
            #res = getURL('http://fund.eastmoney.com/manager/30337449.html')
            soup = BeautifulSoup(res.text, 'html.parser')
            tables = soup.find_all("table")
            if len(tables)>1:
                tab = tables[1]
                manager_his = {}
                for tr in tab.findAll('tr'):
                    if tr.findAll('td'):
                        try:
                            manager_his['manager_id'] = mana_id
                            manager_his['manager_url'] = mana_url
                            manager_his['manager_name'] = mana_name
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
                        except  Exception as e:
                            msg = str("爬取失败(based on manager): [" + mana_id + "] " + mana_url + " %s" % e)
                            # msg = str("爬取失败")
                            logger.exception(msg)
                        try:
                            mySQL.insertData('fund_managers_his', manager_his)
                            msg = str(
                                manager_his['manager_id'] + " " + manager_his['manager_url'] + " " + manager_his['manager_name']+ " " + manager_his['fund_code'] )
                            logger.info(msg)
                        except  Exception as e:
                            msg = str("入库失败(based on manager): [" + mana_id + "] " + mana_url +" %s" % e)
                            logger.exception(msg)
            else:
                logger.info("页面无法打开： "+mana_id + " " + mana_url + " " + mana_name)


    # 获取基金净值数据
    def getFundNav(self, fund_code, update_flag=False):
        '''''
        因为基金列表中是所有基金代码，一般净值型基金和货币基金数据稍有差异，下面根据数据表格长度判断是一般基金还是货币基金，分别入库
        :param fund_code:
        :return:
        '''
        fund_url = 'http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code=' + fund_code + '&page=1&per=20' # http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code=000001&page=1&per=1
        pages = 0
        records = 0
        ## 获取历史净值的总记录数与页数
        try:
            ''''' 
             #获取单个基金的第一页数据，里面返回的apidata 接口中包含了记录数、分页及数据文件等 
             #这里暂按照字符串解析方式获取，既然是标准API接口，应该可以通过更高效的方式批量获取全部净值数据，待后续研究。这里传入基金代码、分页页码和每页的记录数。先简单查询一次获取总的记录数，再一次性获取所有历史净值 
             首次初始化完成后，如果后续每天更新或者定期更新，只要修改下每页返回的记录参数即可 
            '''
            res = getURL(fund_url)
            records = (res.text.strip('var apidata=').strip('{;}').split(',')[1].strip('records:'))
            pages = (res.text.strip('var apidata=').strip('{;}').split(',')[2].strip('pages:'))
        except  Exception as e:
            # print (self.getCurrentTime(),'getFundNav1', fund_code,fund_url,e )
            msg = str("爬取记录总数失败: [" + fund_code + "] " + fund_url + " %s" % e)
            logger.exception(msg)
        # 如增量更新，取60个交易日数据
        if not update_flag:
            pages = int(pages) + 1
        else:
            pages = 3

        ## 根据基金代码和总记录数，分页返回所有历史净值(20 per page)
        i = 0  # 基金总record数
        for pg in range(1, pages):
            fund_nav = 'http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code=' + fund_code + '&page=' + str(pg) + '&per=' + records
            res = getURL(fund_nav)
            soup = BeautifulSoup(res.text, 'html.parser')
            result = {}
            result['fund_code'] = fund_code
            tables = soup.findAll('table')
            tab = tables[0]
            # 解析表格，逐行逐单元格获取净值数据
            for tr in tab.findAll('tr'):
                # 跳过表头；获取净值、累计净值和日收益率数据 如果列数为7，可以判断为一般基金。当然也可以通过标题或者基金类型参数来判断，待后续优化
                if tr.findAll('td') and len((tr.findAll('td'))) == 7:
                    i = i + 1
                    try:
                        result['the_date'] = (
                            tr.select('td:nth-of-type(1)')[0].getText().strip().split(u'起始时间')[0].replace('*','').strip())
                        result['nav'] = (tr.select('td:nth-of-type(2)')[0].getText().strip())
                        result['add_nav'] = (tr.select('td:nth-of-type(3)')[0].getText().strip())
                        result['nav_chg_rate'] = (tr.select('td:nth-of-type(4)')[0].getText().strip())
                        result['buy_state'] = (tr.select('td:nth-of-type(5)')[0].getText().strip())
                        result['sell_state'] = tr.select('td:nth-of-type(6)')[0].getText().strip()
                        result['div_record'] = tr.select('td:nth-of-type(7)')[0].getText().strip().strip('\'')
                    except  Exception as e:
                        msg = str("解析历史记录失败: [" + fund_code + "] " + fund_url + " %s" % e)

                        logger.exception(msg)
                    try:
                        mySQL.insertData('fund_nav', result)
                        msg = str("[" + result['fund_code'] + "] " + str(i) + '/' + str(records) + " " + result[
                            'the_date'] + " " + result['nav'] + " " + result['add_nav'] + " " + result[
                                      'nav_chg_rate'] + " " + result['buy_state'] + " " + result['sell_state'] + " " +
                                  result['div_record'])
                        logger.info(msg)
                    except  Exception as e:
                        msg = str("入库失败: [" + fund_code + "] " + fund_url + " %s" % e)
                        logger.exception(msg)
                # 如果是货币基金，获取万份收益和7日年化利率
                elif tr.findAll('td') and len((tr.findAll('td'))) == 6:
                    i = i + 1
                    try:
                        result['the_date'] = (
                            tr.select('td:nth-of-type(1)')[0].getText().strip().split(u'起始时间')[0].replace('*','').strip())
                        result['profit_per_units'] = (tr.select('td:nth-of-type(2)')[0].getText().strip())
                        result['profit_rate'] = (tr.select('td:nth-of-type(3)')[0].getText().strip())
                        result['buy_state'] = (tr.select('td:nth-of-type(4)')[0].getText().strip())
                        result['sell_state'] = (tr.select('td:nth-of-type(5)')[0].getText().strip())
                        result['div_record'] = (tr.select('td:nth-of-type(6)')[0].getText().strip())
                    except  Exception as e:
                        msg = str("解析历史记录失败: [" + fund_code + "] " + fund_url + " %s" % e)
                        logger.exception(msg)
                    try:
                        mySQL.insertData('fund_nav_currency', result)
                        msg = str("[" + result['fund_code'] + "] " + str(i) + '/' + str(records) + " " + result[
                            'the_date'] + " " + result['profit_per_units'] + " " + result['profit_rate'] + " " + result[
                                      'buy_state'] + " " + result['sell_state'])
                        logger.info(msg)
                    except  Exception as e:
                        msg = str("入库失败: [" + fund_code + "] " + fund_url + " %s" % e)
                        logger.exception(msg)
                else:
                    pass
        msg = str("[" + fund_code + "] " + '共' + str(i) + '/' + str(records) + '行数保存成功')
        logger.info(msg)
        return 0

    # 获取历史净值的总记录数(net)
    def getFundNavQuan(self, fund_code):
        fund_url = 'http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code=' + fund_code + '&page=1&per=1' # http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code=000001&page=1&per=1
        try:
            logger.info("Processing NAV quantity [" + fund_code + "]")
            res = getURL(fund_url)
            records = (res.text.strip('var apidata=').strip('{;}').split(',')[1].strip('records:'))
            return records
        except  Exception as e:
            msg = str("爬取记录总数失败: [" + fund_code + "] " + fund_url + " %s" % e)
            logger.exception(msg)
            return 1


# 更新库表
def updateTable(tab):
    try:
        mySQL.distinctTable(tab)
    except Exception as e:
        msg = str('UPDATE %s FAIL:\n%s' % (tab, e))
        logger.exception(msg)


# 检查净值数据
def checkNAV(funds):
    try:
        fundsCheck = pd.DataFrame(data=funds, columns=['fund_code'])
        start = time.time()
        fundsCheck['quantity'] = fundsCheck.apply(lambda x: fundSpiders.getFundNavQuan(x['fund_code']),axis=1)  # from internet
        end = time.time()
        msg = "Time used to scrape NAV quantity: " + str(end - start)
        logger.info(msg)

        df_nav = mySQL.queryNAVQuantity('fund_nav')
        df_nav_c = mySQL.queryNAVQuantity('fund_nav_currency')  # from DB
        start = time.time()
        msg = "Time used to query NAV quantity from DB: " + str(start - end)
        logger.info(msg)

        df_nav.columns = ['fund_code', 'quantityFromDB']
        df_nav_c.columns = ['fund_code', 'quantityFromDB']
        df_nav = pd.concat([df_nav, df_nav_c], axis=0)
        fundsCheck = pd.merge(fundsCheck, df_nav, how='left', on=['fund_code'])
        end = time.time()
        msg = "Time used to merge NAV quantity: " + str(end - start)
        logger.info(msg)

        fundsCheck['pct'] = fundsCheck.apply(
            lambda x: pd.to_numeric(x['quantityFromDB'], errors='coerce') / pd.to_numeric(x['quantity'],errors='coerce'), axis=1)
        fundsCheck['diff'] = fundsCheck.apply(
            lambda x: pd.to_numeric(x['quantity'], errors='coerce') - pd.to_numeric(x['quantityFromDB'],errors='coerce'), axis=1)
        fundsCheck.to_csv('../out/1-fundsCheck.csv', index=False)
    except Exception as e:
        msg = str('NAV QUANTITY CHECK FAIL: ' + " %s" % e)
        logger.exception(msg)


# 导出表格为csv
def exportTable(tab):
    try:
        pd.DataFrame(mySQL.queryData(tab)).to_csv('../out/1-%s.csv' % tab,index=False)
        msg = str('EXPORT TABLES %s Succeed!' % tab)
        logger.info(msg)
    except Exception as e:
        msg = str('EXPORT TABLES %s Fail:\n%s' % (tab, e))
        logger.exception(msg)


# 功能选择对话
def welcome():
    logger.info('\nPlz input no. to execute commands:\n'
                '1.Update all FUND INFO & MANAGER INFO(& export)\n'
                '2.Update all NAV INFO(& export)\n'
                '3.Update last 60 days NAV INFO(& export)\n'
                '4.Just Tidy up NAV INFO\n'
                '5.Just Check NAV INFO\n'
                '6.Export all TABLES\n'
                '9.Exit\n'
                '\n'
                'Command NO.: ')
    n = int(sys.stdin.readline().strip('\n'))
    if n in [1, 2, 3, 4, 5, 7, 9]:
        return n
    else:
        logger.info('INVALID INPUT\n')
        welcome()


# MAIN
def main():
    global mySQL, fundSpiders, sleep_time, isproxy, proxy, header
    mySQL = PyMySQL()
    mySQL._init_('localhost', 'root', 'JONC', 'fund')  # host/user/password/database
    isproxy = 0  # 如需要使用代理，改为1，并设置代理IP参数 proxy  
    proxy = {"http": "http://110.37.84.147:8080", "https": "http://110.37.84.147:8080"}  # 这里需要替换成可用的代理IP
    header = randHeader()
    sleep_time = 0.1
    fundSpiders = FundSpiders()

    funds = fundSpiders.getFundCodesFromCsv()
    fund_count = len(funds)
    count = 0

    while True:
        n = welcome()
        if n == 1:
            # 1.Update all FUND INFO & MANAGER INFO(& export)
            # truncate
            for tab in ['fund_info', 'fund_managers_info', 'fund_managers_chg','fund_managers_his']:
                mySQL.truncateTable(tab)
            # scape & import
            for fund in funds:
                count = count + 1
                logger.info("\nProcessing [%d/%d] Funds" % (count, fund_count))
                try:
                    fundSpiders.getFundInfo(fund)  # fund info 全量
                except Exception as e:
                    msg = str('GET FUND INFO FROM EASTMONEY [' + fund + '] \n' + " %s" % e)
                    logger.exception(msg)
                try:
                    fundSpiders.getFundManagers(fund)  # manager info, 全量
                except Exception as e:
                    msg = str('GET MANAGER INFO FROM EASTMONEY [' + fund + '] \n' + " %s" % e)
                    logger.exception(msg)
            # export
            for tab in ['fund_info', 'fund_managers_info', 'fund_managers_chg']:
                exportTable(tab)
            # scape & import
            fundSpiders.getFundManagersHistory()  # manager history, 全量
            # export
            exportTable('fund_managers_his')
        elif (n == 2 or n == 3):
            # 2.Update all NAV INFO(& export)/3.Update last 60 days NAV INFO(& export)
            # scape & import
            flag = (False if n == 2 else True)
            for fund in funds:
                count = count + 1
                logger.info("\nProcessing [%d/%d] Funds" % (count, fund_count))
                try:
                    fundSpiders.getFundNav(fund, update_flag=flag)  # nav info, update_flag = False(全量)/True(增量)
                except Exception as e:
                    msg = str('GET NAV INFO FROM EASTMONEY [' + fund + '] \n' + " %s" % e)
                    logger.exception(msg)
            # update
            for tab in ['fund_nav', 'fund_nav_currency']:
                updateTable(tab)
            # check
            checkNAV(funds)
            # export
            for tab in ['fund_nav', 'fund_nav_currency']:
                exportTable(tab)
        elif n == 4:
            # 4.Just Tidy up NAV INFO
            # update
            for tab in ['fund_nav', 'fund_nav_currency']:
                updateTable(tab)
            # export
            for tab in ['fund_nav', 'fund_nav_currency']:
                exportTable(tab)
        elif n == 5:
            # 5.Just Check NAV INFO
            # check
            checkNAV(funds)
        elif n == 6:
            # 6.Export all TABLES
            # export
            for tab in ['fund_info', 'fund_managers_info', 'fund_managers_chg', 'fund_nav', 'fund_nav_currency']:
                exportTable(tab)
        elif n == 7:
            logger.info('Preserved SECTION')
        else:
            logger.info('Bye~')
            break

    ### close DB connection
    mySQL.cur.close()


if __name__ == "__main__":
    main()
