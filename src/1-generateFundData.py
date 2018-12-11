# -*- coding:utf-8 -*-  
############################################################################  
''''' 
# 程序：东方财富网基金数据爬取 
# 功能：抓取东方财富网上基金相关数据 
# 创建时间：2017/02/14 基金概况数据 
# 更新历史：2017/02/15 增加基金净值数据 
# 使用库：requests、BeautifulSoup4、pymysql,pandas 
# 作者：yuzhucu 
'''  
#############################################################################  
import requests  
from bs4 import BeautifulSoup  
import time  
import random  
import pymysql  
import os  
import pandas as pd  
# import re  
import logging
 
logging.basicConfig(level=logging.INFO,
                    filename='../log/1-generateFundData.log',
                    filemode='a',
                    datefmt='%Y/%m/%d %H:%M:%S',
                    # format='%(levelname)s %(asctime)s %(lineno)d %(message)s'
                    format='%(levelname)s %(asctime)s %(funcName)s %(lineno)d %(message)s'
                    )
logger = logging.getLogger(__name__)

def randHeader():  
    ''''' 
    随机生成User-Agent 
    :return: 
    '''  
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
  
def getCurrentTime():  
        # 获取当前时间  
        return time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time()))  
  
def getURL(url, tries_num=5, sleep_time=0, time_out=10,max_retry = 5):  
        ''''' 
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
        except requests.RequestException as e:  
            sleep_time_p = sleep_time_p + 10  
            time_out_p = time_out_p + 10  
            tries_num_p = tries_num_p - 1  
            # 设置重试次数，最大timeout 时间和 最长休眠时间  
            if tries_num_p > 0:  
                time.sleep(sleep_time_p)  
                # print (getCurrentTime(), url, 'URL Connection Error: 第', max_retry - tries_num_p, u'次 Retry Connection', e)  
                msg = str(url+'URL Connection Error: 第'+ max_retry - tries_num_p+'次 Retry Connection. %s' %e)
                print(msg)
                logger.exception(msg)
                return getURL(url, tries_num_p, sleep_time_p, time_out_p,max_retry)  
        return res  
  
class PyMySQL:  
    # 获取当前时间  
    def getCurrentTime(self):  
        return time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time()))  
    # 数据库初始化  
    def _init_(self, host, user, passwd, db,port=3306,charset='utf8'):  
        pymysql.install_as_MySQLdb()  
        try:  
            self.db =pymysql.connect(host=host,user=user,passwd=passwd,db=db,port=3306,charset='utf8')  
            #self.db = pymysql.connect(ip, username, pwd, schema,port)  
            self.db.ping(True)#使用mysql ping来检查连接,实现超时自动重新连接
            msg = "MySQL DB Connect Success: "+user+'@'+host+':'+str(port)+'/'+db
            print(msg)
            logger.info(msg)
            self.cur = self.db.cursor()  
        except  Exception as e:  
            msg = str("MySQL DB Connect Error: %s" %e)
            print(msg)
            logger.exception(msg)
    # 插入数据  
    def insertData(self, table, my_dict):  
        try:  
            #self.db.set_character_set('utf8')  
            cols = ', '.join(my_dict.keys())  
            values = '","'.join(my_dict.values())  
            sql = "replace into %s (%s) values (%s)" % (table, cols, '"' + values + '"')  
            #print (sql)  
            try:  
                result = self.cur.execute(sql)  
                insert_id = self.db.insert_id()  
                self.db.commit()  
                # 判断是否执行成功  
                if result:  
                    #print (self.getCurrentTime(), u"Data Insert Sucess")  
                    return insert_id  
                else:  
                    return 0  
            except Exception as e:  
                # 发生错误时回滚  
                self.db.rollback()  
                # print (self.getCurrentTime(), u"Data Insert Failed: %s" % (e)) 
                msg = str("Data Insert Failed: %s" % e)
                print(msg)
                logger.exception(msg)             
                return 0  
        except Exception as e:  
            # print (self.getCurrentTime(), u"MySQLdb Error: %s" % (e))  
            msg = str("MySQLdb Error: %s" %e)
            print(msg)
            logger.exception(msg)
            return 0  
  
class FundSpiders():  
  
    def getCurrentTime(self):  
        # 获取当前时间  
        return time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time()))  
  
    def getFundCodesFromCsv(self):  
        ''''' 
        从csv文件中获取基金代码清单（可从wind或者其他财经网站导出） 
        '''  
        # file_path=os.path.join(os.getcwd(),'fundCode.csv')  
        file_path ="../dep/fundCode.csv"
        # fund_code = pd.read_csv(file_path,encoding='gbk')  
        fund_code = pd.read_csv(file_path,dtype=str) 
        # 在trade_code 列
        Code = fund_code.trade_code  
        return Code  
  
    def getFundInfo(self,fund_code,mode = 'default'):  
        ''' 
        获取基金概况基本信息 
        :param fund_code: 
        :return: 
        '''  
        fund_url='http://fund.eastmoney.com/f10/jbgk_'+fund_code +'.html'  
        res = getURL(fund_url)  
        soup = BeautifulSoup(res.text, 'html.parser')  
        result = {}  
        try:  
            ''' 
           之前用select、find 比较多，但是一些网页中经常出现部分字段不全导致内容和数据库不匹配的情况导致数据错位。这里改为用使用标题的next_element 来获取数据值来规避此问题 
           其中也有个别字段有问题的，特殊处理下即可 
            '''  
            result['fund_code']=fund_code  
            result['fund_name']= soup.find_all(text=u"基金全称")[0].next_element.text.strip()  
            result['fund_abbr_name']= soup.find_all(text=u"基金简称")[0].next_element.text.strip()  
            #result['fund_code']= soup.find_all(text=u"基金代码")[0].next_element )  
            result['fund_type']= soup.find_all(text=u"基金类型")[0].next_element.text.strip()  
            result['issue_date']= soup.find_all(text=u"发行日期")[0].next_element.text.strip()  
            result['establish_date']= soup.find_all(text=u"成立日期/规模")[0].next_element.text.split(u'/')[0].strip()  
            result['establish_scale']= soup.find_all(text=u"成立日期/规模")[0].next_element.text.split(u'/')[-1].strip() 
            result['asset_value']= soup.find_all(text=u"资产规模")[0].next_element.text.split(u'（')[0].strip()
            if result['asset_value'] !="---":
                result['asset_value_date'] = soup.find_all(text=u"资产规模")[0].next_element.text.split(u'（')[1].split(u'）')[0].strip(u'截止至：')  
            else:
                result['asset_value_date'] = result['asset_value']
            result['units']= soup.find_all(text=u"份额规模")[0].next_element.text.strip().split(u'（')[0].strip()  
            if result['units'] !="---":
                result['units_date'] = soup.find_all(text=u"份额规模")[0].next_element.text.strip().split(u'（')[1].strip(u'（截止至：）')  
            else:
                result['units_date'] = result['units']
            result['fund_manager']= soup.find_all(text=u"基金管理人")[0].next_element.text.strip()  
            result['fund_trustee']= soup.find_all(text=u"基金托管人")[0].next_element.text.strip()  
            result['funder']= soup.find_all(text=u"基金经理人")[0].next_element.text.strip()  
            result['total_div']= soup.find_all(text=u"成立来分红")[0].next_element.text.strip()  
            result['mgt_fee']= soup.find_all(text=u"管理费率")[0].next_element.text.strip()  
            result['trust_fee']= soup.find_all(text=u"托管费率")[0].next_element.text.strip()  
            result['sale_fee']= soup.find_all(text=u"销售服务费率")[0].next_element.text.strip()  
            result['buy_fee']= soup.find_all(text=u"最高认购费率")[0].next_element.text.strip().split(u'（')[0].strip()    
            result['buy_fee2']= soup.find_all(text=u"最高申购费率")[0].next_element.text.strip().split(u'（')[0].strip()    
            result['benchmark']= soup.find_all(text=u"业绩比较基准")[0].next_element.text.strip(u'该基金暂未披露业绩比较基准')  
            result['underlying']= soup.find_all(text=u"跟踪标的")[0].next_element.text.strip(u'该基金无跟踪标的')  
        except  Exception as e:  
            # print (self.getCurrentTime(),'getFundInfo|爬取失败:\n', fund_code,fund_url,e)  
            msg = str("爬取失败: [" + fund_code+"] "+fund_url+" %s" %e)
            print(msg)
            logger.exception(msg)
  
        try:  
            mySQL.insertData('fund_info', result)  
            #print (self.getCurrentTime(),'Fund Info Insert Sucess:', result['fund_code'],result['fund_name'],result['fund_abbr_name'],result['fund_manager'],result['funder'],result['establish_date'],result['establish_scale'],result['benchmark'] )  
            # print (self.getCurrentTime(),'getFundInfo:', result['fund_code'],result['fund_name'],result['fund_abbr_name'],result['fund_manager'],result['funder'],result['establish_date'] )  
            msg = str(result['fund_code']+" "+result['fund_name']+" "+result['fund_abbr_name']+" "+result['fund_manager']+" "+result['funder']+" "+result['establish_date'] )
            logger.info(msg)
        except  Exception as e:  
            # print (self.getCurrentTime(),'getFundInfo|入库失败:\n',fund_code,fund_url,e)  
            msg = str("入库失败: [" + fund_code+"] "+fund_url+" %s" %e)
            print(msg)
            logger.exception(msg)
  
        return result  
  
    def getFundManagers(self,fund_code):  
        ''''' 
        获取基金经理数据。 基金投资分析关键在投资经理，后续在完善 
        :param fund_code: 
        :return: 
        '''  
        fund_url='http://fund.eastmoney.com/f10/jjjl_'+fund_code +'.html'  
        res = getURL(fund_url)  
        soup = BeautifulSoup(res.text, 'html.parser')  
        result = {}  
        manager={}  
        tables=soup.find_all("table")  
        tab = tables[1]  
        #print (tables[1])  
        i=0  
        #先用本办法，解析表格，逐行逐单元格获取净值数据  
        for tr in tab.findAll('tr'):  
            #跳过表头；获取净值、累计净值和日收益率数据 如果列数为7，可以判断为一般基金。当然也可以通过标题或者基金类型参数来判断，待后续优化  
            if tr.findAll('td') :#and len((tr.findAll('td')))==7 :  
                i=i+1  
                try:  
                     result['fund_code']=fund_code  
                     result['start_date']= tr.select('td:nth-of-type(1)')[0].getText().strip()  
                     result['end_date']= tr.select('td:nth-of-type(2)')[0].getText().strip()  
                     result['fund_managers']= tr.select('td:nth-of-type(3)')[0].getText().strip()  
                     result['term']= tr.select('td:nth-of-type(4)')[0].getText().strip()  
                     result['return_rate']= tr.select('td:nth-of-type(5)')[0].getText().strip('%')+'%'  
                     result['created_date']=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))  
                     result['updated_date']=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))  
                     result['data_source']='eastmoney'  
  
                except  Exception as e:  
                     # print (self.getCurrentTime(),'getFundBasedManagers|爬取失败:\n', fund_code,fund_url,e )  
                     msg = str("爬取失败(based on fund): [" + fund_code+"] "+fund_url+" %s" %e)
                     print(msg)
                     logger.exception(msg)
  
                try:  
                    mySQL.insertData('fund_managers_chg', result)  
                    # print (self.getCurrentTime(),'fund_managers_chg:',result['fund_code'],i,result['start_date'],result['end_date'],result['fund_managers'],result['term'],result['return_rate'] )  
                    msg = str(result['fund_code']+" "+result['start_date']+" "+result['end_date']+" "+result['fund_managers']+" "+result['term']+" "+result['return_rate'])
                    logger.info(msg)
                except  Exception as e:  
                    # print (self.getCurrentTime(),'getFundBasedManagers|入库失败:\n', fund_code,fund_url,e )  
                    msg = str("入库失败(based on fund): [" + fund_code+"] "+fund_url+" %s" %e)
                    print(msg)
                    logger.exception(msg)
  
                for a in tr.findAll('a'):  
                    if a:  
                        try:  
                            manager['manager_id']=a['href'].strip('http://fund.eastmoney.com/manager/.html')  
                            manager['url']=a['href']  
                            manager['manager_name']=a.text  
                            manager['created_date']=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))  
                            manager['updated_date']=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))  
                            manager['data_source']='eastmoney'  
                            #print (self.getCurrentTime(),manager['manager_id'],manager['manager_name'],manager['url'])  
                        except Exception as e:  
                            # print (self.getCurrentTime(),'getFundManagers3', fund_code,manager['manager_name'],manager['url'],fund_url,e )  
                            msg = str("爬取失败(based on persons): [" + fund_code+"] "+manager['manager_name']+" "+manager['url']+" "+fund_url+" %s" %e )
                            print(msg)
                            logger.exception(msg)
  
                        try:  
                            mySQL.insertData('fund_managers_info', manager)  
                            # print (self.getCurrentTime(),'fund_managers_info:',fund_code,manager['manager_name'],manager['url'],manager['manager_id'] )  
                            msg = str(fund_code+" "+manager['manager_name']+" "+manager['url']+" "+manager['manager_id'] )  
                            logger.info(msg)
                        except  Exception as e:  
                            # print (self.getCurrentTime(),'getFundManagers4', fund_code,fund_url,e )  
                            msg = str("入库失败(based on persons): [" + fund_code+"] "+manager['manager_name']+" "+manager['url']+" "+fund_url+" %s" %e)
                            print(msg)
                            logger.exception(msg)
        #print (self.getCurrentTime(),'getFundManagers',result['fund_code'],'共',str(i)+':','行数保存成功'   )  
  
        return result  
  
    def getFundNav(self,fund_code):  
        ''''' 
        获取基金净值数据，因为基金列表中是所有基金代码，一般净值型基金和货币基金数据稍有差异，下面根据数据表格长度判断是一般基金还是货币基金，分别入库 
        :param fund_code: 
        :return: 
        '''  
        try:  
             #http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code=000001&page=1&per=1  
             ''''' 
             #寿险获取单个基金的第一页数据，里面返回的apidata 接口中包含了记录数、分页及数据文件等 
             #这里暂按照字符串解析方式获取，既然是标准API接口，应该可以通过更高效的方式批量获取全部净值数据，待后续研究。这里传入基金代码、分页页码和每页的记录数。先简单查询一次获取总的记录数，再一次性获取所有历史净值 
             首次初始化完成后，如果后续每天更新或者定期更新，只要修改下每页返回的记录参数即可 
            '''  
             fund_url='http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code='+fund_code +'&page=1&per=1'  
             res = getURL(fund_url)  
             #获取历史净值的总记录数  
             records= (res.text.strip('var apidata=').strip('{;}').split(',')[1].strip('records:'))  
             # pages = (res.text.strip('var apidata=').strip('{;}').split(',')[2].strip('records:'))  
             #print(res.text.strip('var apidata=').strip('{;}').split(','))  
             #print (records)  
        except  Exception as e:  
            # print (self.getCurrentTime(),'getFundNav1', fund_code,fund_url,e )  
            msg = str("爬取记录总数失败: [" + fund_code+"] "+fund_url+" %s" %e)
            print(msg)
            logger.exception(msg)
        try:  
            #根据基金代码和总记录数，一次返回所有历史净值  
            fund_nav='http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code='+fund_code +'&page=1&per='+records  
            res = getURL(fund_nav)  
            soup = BeautifulSoup(res.text, 'html.parser')  
        except  Exception as e:  
            # print (self.getCurrentTime(),'getFundNav2', fund_code,fund_url,e )  
            msg = str("爬取历史记录失败: [" + fund_code+"] "+fund_url+" %s" %e)
            print(msg)
            logger.exception(msg)
  
        result={}  
        result['fund_code']=fund_code  
        tables = soup.findAll('table')  
        tab = tables[0]  
        i=0  
        #先用本办法，解析表格，逐行逐单元格获取净值数据  
        for tr in tab.findAll('tr'):  
            #跳过表头；获取净值、累计净值和日收益率数据 如果列数为7，可以判断为一般基金。当然也可以通过标题或者基金类型参数来判断，待后续优化  
            if tr.findAll('td') and len((tr.findAll('td')))==7 :  
                i=i+1  
                try:  
                     result['the_date']= (tr.select('td:nth-of-type(1)')[0].getText().strip().split(u'起始时间')[0].replace('*','').strip() )  
                     result['nav']= (tr.select('td:nth-of-type(2)')[0].getText().strip() )  
                     result['add_nav']= (tr.select('td:nth-of-type(3)')[0].getText().strip() )  
                     result['nav_chg_rate']= (tr.select('td:nth-of-type(4)')[0].getText().strip() )  
                     result['buy_state']= (tr.select('td:nth-of-type(5)')[0].getText().strip() )  
                     result['sell_state']= tr.select('td:nth-of-type(6)')[0].getText().strip()  
                     result['div_record']= tr.select('td:nth-of-type(7)')[0].getText().strip().strip('\'')  
                     #print (self.getCurrentTime(),i,result['fund_code'],result['the_date'],result['nav'],result['add_nav'],result['nav_chg_rate'],result['buy_state'],result['sell_state'] )  

                except  Exception as e:  
                     # print (self.getCurrentTime(),'getFundNav3', fund_code,fund_url,e )  
                     msg = str("解析历史记录失败: [" + fund_code+"] "+fund_url+" %s" %e)
                     print(msg)
                     logger.exception(msg)
                try:  
                    mySQL.insertData('fund_nav', result)  
                    msg = str("["+result['fund_code']+"] "+str(i)+'/'+str(records)+" "+result['the_date']+" "+result['nav']+" "+result['add_nav']+" "+result['nav_chg_rate']+" "+result['buy_state']+" "+result['sell_state']+" "+result['div_record'] )
                    logger.info(msg)
                except  Exception as e:  
                    # print (self.getCurrentTime(),'getFundNav4', fund_code,fund_url,e ) 
                    msg = str("入库失败: [" + fund_code+"] "+fund_url+" %s" %e)
                    print(msg)
                    logger.exception(msg)
            #如果是货币基金，获取万份收益和7日年化利率  
            elif  tr.findAll('td') and len((tr.findAll('td')))==6:  
                i=i+1  
                try:  
                     result['the_date']= (tr.select('td:nth-of-type(1)')[0].getText().strip().split(u'起始时间')[0].replace('*','').strip() )  
                     result['profit_per_units']= (tr.select('td:nth-of-type(2)')[0].getText().strip() )  
                     result['profit_rate']= (tr.select('td:nth-of-type(3)')[0].getText().strip() )  
                     result['buy_state']= (tr.select('td:nth-of-type(4)')[0].getText().strip() )  
                     result['sell_state']= (tr.select('td:nth-of-type(5)')[0].getText().strip() )  
                     result['div_record']= (tr.select('td:nth-of-type(6)')[0].getText().strip() )  
                     #print (self.getCurrentTime(),i,result['fund_code'],result['the_date'],result['nav'],result['add_nav'],result['nav_chg_rate'],result['buy_state'],result['sell_state'] )  
                except  Exception as e:  
                     # print (self.getCurrentTime(),'getFundNav5', fund_code,fund_url,e )  
                     msg = str("解析历史记录失败: [" + fund_code+"] "+fund_url+" %s" %e)
                     print(msg)
                     logger.exception(msg)
                try:  
                    mySQL.insertData('fund_nav_currency', result)  
                    # print (self.getCurrentTime(),'fund_nav_currency',str(i)+'/'+str(records),result['fund_code'],result['the_date'],result['profit_per_units'],result['profit_rate'],result['buy_state'],result['sell_state'] )  
                    msg = str("["+result['fund_code']+"] "+str(i)+'/'+str(records)+" "+result['the_date']+" "+result['profit_per_units']+" "+result['profit_rate']+" "+result['buy_state']+" "+result['sell_state'] )  
                    logger.info(msg)
                except  Exception as e:  
                    # print (self.getCurrentTime(),'getFundNav6', fund_code,fund_url,e )  
                    msg = str("入库失败: [" + fund_code+"] "+fund_url+" %s" %e)
                    print(msg)
                    logger.exception(msg)
            else :  
                pass  
            # if i>=1:  
            #     break  
        # print (self.getCurrentTime(),'getFundNav',result['fund_code'],'共',str(i)+'/'+str(records),'行数保存成功'   )  
        msg = str("[" + result['fund_code']+"] "+'共'+str(i)+'/'+str(records)+'行数保存成功' )
        print(msg)
        logger.info(msg)  
        return result  
  
def main():  
    global mySQL, sleep_time, isproxy, proxy, header  
    mySQL = PyMySQL()  
    fundSpiders=FundSpiders()
    
    # host/user/password/database
    mySQL._init_('localhost', 'root', 'JONC', 'fund')  
    isproxy = 0  # 如需要使用代理，改为1，并设置代理IP参数 proxy  
    proxy = {"http": "http://110.37.84.147:8080", "https": "http://110.37.84.147:8080"}#这里需要替换成可用的代理IP  
    header = randHeader()  
    sleep_time = 0.1  
    
    funds=fundSpiders.getFundCodesFromCsv()  
    count= 0
    fund_count = len(funds)
    for fund in funds:  
        count = count+1
        print("\nProcessing [%d/%d] Funds" %(count,fund_count))
        try:
            # core
            # fundSpiders.getFundInfo(fund)  
            # fundSpiders.getFundManagers(fund)  
            fundSpiders.getFundNav(fund)  
        except Exception as e:  
            print (getCurrentTime(),'main', fund,e )              
    mySQL.cur.close()
    
if __name__ == "__main__":  
    main()  
