import getpass
import logging

import pandas as pd
import pymysql

logger = logging.getLogger('main.PyMySQL')

class PyMySQL:
    # 数据库初始化
    def _init_(self, host, user, passwd, db, port=3306, charset='utf8'):
        pymysql.install_as_MySQLdb()
        try:
            self.conn = pymysql.connect(host=host, user=user, passwd=passwd, db=db, port=3306, charset=charset)
            self.conn.ping(reconnect=True)  # 使用mysql ping来检查连接,实现超时自动重新连接
            self.conn.autocommit(True)
            self.cur = self.conn.cursor()
            logger.info("MySQL DB Connect Success: " + user + '@' + host + ':' + str(port) + '/' + db)
        except Exception as e:
            logger.exception(str("MySQL DB Connect Error: %s" % e))
            return 1

    # 插入数据
    def insertData(self, table, my_dict):
        try:
            cols = ', '.join(my_dict.keys())
            values = '","'.join(my_dict.values())
            sql = "replace into %s (%s) values (%s)" % (table, cols, '"' + values + '"')
            result = self.cur.execute(sql) # return rows affected
            self.checkStatus(result)
        except Exception as e:
            self.conn.rollback()  # 发生错误时回滚
            logger.exception(str("Data Insert Failed: %s" % e))
            return 1

    # 批量插入数据 TODO batch insert
    def executeManyData(self, table, my_dict):
        try:
            cols = ', '.join(my_dict[0].keys())
            len_dict = len(my_dict[0])
            for i in range(len(my_dict)):
                my_dict[i] = tuple(my_dict[i])
            sql = "replace into %s (%s) values (" % (table, cols) + '%s,' * (len_dict - 1) + '%s)'
            result = self.cur.executemany(sql, my_dict)  # return rows affected
            self.checkStatus(result)
        except Exception as e:
            self.conn.rollback()  # 发生错误时回滚
            logger.exception(str("Data Insert Failed: %s" % e))
            return 1

    # 查询数据
    def queryData(self, table):
        try:
            sql = 'select DISTINCT * from %s' % table
            self.cur.execute(sql)
            data = self.cur.fetchall()
            dict = []
            [dict.append(field[0]) for field in self.cur.description]
            df = pd.DataFrame(list(data), columns=dict)
            logger.info(str("Successfully retrieved " + str(self.cur.execute(sql)) + " records from %s" % table))
            return df
        except Exception as e:
            logger.exception(str("Fail to retrieve data from %s" % table))
            return 1

    # 建表
    def createTable(self, table, sourceTab, mode = 'like'):
        sql=''
        if mode == 'like':
            sql = "create table %s like %s" % (table, sourceTab)
        else:
            logger.exception('More create table mode TBC~')
        try:
            logger.info('Executing %s' % sql)
            result = self.cur.execute(sql)
            self.checkStatus(result)
            logger.info(str("Table %s Create Succeed!" % table))
        except Exception as e:
            self.conn.rollback()
            logger.exception(str("Table %s Create Failed: %s" % (table, e)))
            return 1

    # 删除重复表数据| DISCARD
    def distinctTable(self, table):
        newTableName = table + '_unique'
        dropIfExist = "drop table if exists %s" % newTableName
        sql = "create table %s as select distinct * from %s" % (newTableName, table)
        try:
            self.cur.execute(dropIfExist)
            result = self.cur.execute(sql)
            self.checkStatus(result)
            logger.info(str("Table %s Select Distinct Succeed!" % table))
        except Exception as e:
            self.conn.rollback()
            logger.exception(str("Table %s Select Distinct Failed: %s" % (table, e)))
            return 1

    # 清空表数据| DISCARD
    def truncateTable(self, table):
        sql = "truncate table %s" % table
        try:
            logger.info('Executing %s' % sql)
            result = self.cur.execute(sql)
            self.checkStatus(result)
            logger.info(str("Table %s Truncate Succeed!" % table))
        except Exception as e:
            self.conn.rollback()
            logger.exception(str("Table %s Truncate Failed: %s" % (table, e)))
            return 1


    # 删表
    def dropTable(self, table):
        cfm = bool(input('Plz confirm to DROP table %s :'%table))
        if cfm :
            sql = "drop table if exists %s" % table
            try:
                logger.info('Executing %s' % sql)
                result = self.cur.execute(sql)
                self.checkStatus(result)
                logger.info(str("Table %s Drop Succeed!" % table))
            except Exception as e:
                self.conn.rollback()
                logger.exception(str("Table %s Drop Failed: %s" % (table, e)))
                return 1
        else:
            logger.exception('STOPPED BY YOU')

    # 通用查询
    def sql(self, sql):
        try:
            # sql = "select distinct fund_code, count(*) from (select distinct * from %s) t group by fund_code" % table
            self.cur.execute(sql)
            data = self.cur.fetchall()
            dict= []
            [dict.append(field[0]) for field in self.cur.description]
            df = pd.DataFrame(list(data), columns=dict)
            logger.info(str("Successfully execute %s" % sql))
            return df
        except Exception as e:
            logger.exception(str("Fail to execute %s" % sql))
            return 1

    # 迁移库表
    def migrateTable(self, dest, source):
        try:
            sql = "replace into %s select * from %s" % (dest,source)
            result = self.cur.execute(sql)
            self.checkStatus(result)
            logger.info(str("Table %s to %s Drop Succeed!" % (source,dest)))
        except Exception as e:
            self.conn.rollback()
            logger.exception(str("Table Migrate Failed: %s" % e))
            return 1

    def checkStatus(self,rowAffect):
        # insert_id = self.conn.insert_id() # 0 if OK
        # if result:  # 判断是否执行成功
        #     return insert_id
        # else:
        #     return 1
        if not rowAffect:  # 判断是否执行成功
            return 1


    #释放资源
    def dispose(self):
        self.conn.close()
        self.cur.close()


def welcome():
    try:
        sql = input('Input SQL: ')
        if sql.upper() =='EXIT':
            mySQL.dispose()
            exit()
        else:
            mySQL.sql(sql)
    except:
        logger.info('INVALID INPUT\n')
    welcome()

if __name__ == "__main__":
    # global mySQL
    logger.info('Login to MySQL...')
    db = input('Database: ')
    user = getpass.getuser()
    passwd = getpass.getpass(prompt='Password: ')

    mySQL = PyMySQL()
    mySQL._init_('localhost', user, passwd, db)  # host/user/password/database

    welcome()

