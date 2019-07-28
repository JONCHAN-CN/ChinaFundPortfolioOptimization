import getpass
import logging

import pandas as pd
import pymysql

logger = logging.getLogger('main.PyMySQL')


def _exception(function):
    def wrapper(self, *args, **kwargs):
        try:
            return function(self, *args, **kwargs)  # core function
        except Exception as e:
            self.conn.rollback()
            logger.exception(str(e))
            return 1

    return wrapper


class PyMySQL:
    def _init_(self, host, user, passwd, db, port, charset):
        pymysql.install_as_MySQLdb()
        self.conn = pymysql.connect(host=host, user=user, passwd=passwd, db=db, port=port, charset=charset)
        self.conn.ping(reconnect=True)  # 使用mysql ping来检查连接,实现超时自动重新连接
        self.conn.autocommit(True)
        self.cur = self.conn.cursor()
        logger.info("\nMySQL Connect Success - %s@%s:%s/%s" % (user, host, port, db))

    @_exception
    def sql(self, sql):  # not necessary
        nrows = self.cur.execute(sql)  # return rows affected
        if 'select' in sql:
            data = self.cur.fetchall()
            dict = []
            [dict.append(field[0]) for field in self.cur.description]
            df = pd.DataFrame(list(data), columns=dict)
            return df
        else:
            return nrows

    @_exception
    def insertData(self, table, my_dict):
        cols = ', '.join(my_dict.keys())
        values = '"' + '","'.join(my_dict.values()) + '"'
        sql = "replace into %s (%s) values (%s)" % (table, cols, values)
        nrows = self.cur.execute(sql)
        self.checkStatus(nrows)

    @_exception
    def executeManyData(self, table, my_dict_list):
        data = []
        for i in my_dict_list:
            cols = ', '.join(i.keys())
            vals = '"' + '","'.join(i.values()) + '"'
            tup_ = tuple(table, cols, vals)
            data = data.append(tup_)
        sql = "replace into %s (%s) values (%s)"
        nrows = self.cur.executemany(sql, data)
        self.checkStatus(nrows)

    @_exception
    def selectDistinct(self, table, where=''):
        sql = "select distinct * from %s %s" % (table, where)
        nrows = self.cur.execute(sql)
        data = self.cur.fetchall()
        dict = []
        [dict.append(field[0]) for field in self.cur.description]
        df = pd.DataFrame(list(data), columns=dict)
        logger.info("Retrieved %d records from %s" % (nrows, table))
        return df

    @_exception
    def createTable(self, table, where):
        sql = "create table %s %s" % (table, where)
        nrows = self.cur.execute(sql)
        self.checkStatus(nrows)
        logger.info(str("Create table %s succeed" % table))

    @_exception
    def truncateTable(self, table):
        sql = "truncate table %s" % table
        nrows = self.cur.execute(sql)
        self.checkStatus(nrows)
        logger.info(str("Truncate table %s Succeed!" % table))

    @_exception
    def dropTable(self, table):
        cfm = bool(input('Plz confirm to DROP table %s :' % table))
        if cfm:
            sql = "drop table if exists %s" % table
            nrows = self.cur.execute(sql)
            self.checkStatus(nrows)
            logger.info(str("Drop table %s Succeed!" % table))

    @_exception
    def distinctTable(self, table, index=None):
        if index is None:
            newTab = table + '_unique'
            self.dropTable(newTab)
            self.createTable(newTab, 'as select distinct * from %s' % table)
            self.dropTable(table)
            self.sql('ALTER TABLE %s RENAME TO %s' % (newTab, table))
            logger.info(str("Select distinct from table %s Succeed!" % table))
        else:
            print('TBC')

    @_exception
    def mergeTable(self, dest, source):
        sql = "replace into %s select distinct * from %s" % (dest, source)
        nrows = self.cur.execute(sql)
        self.checkStatus(nrows)
        logger.info(str("Merge table %s to %s Succeed!" % (source, dest)))

    def checkStatus(self, rowAffect):
        if not rowAffect:  # 判断是否执行成功
            return 1

    # 释放资源
    def dispose(self):
        self.conn.close()
        self.cur.close()


def welcome():
    try:
        sql = input('Input SQL: ')
        if sql.upper() == 'EXIT':
            engine.dispose()
            exit()
        else:
            engine.sql(sql)
    except:
        logger.info('INVALID INPUT\n')
    welcome()


if __name__ == "__main__":
    logger.info('Login to MySQL...')
    host = input('host: ')
    db = input('database: ')
    user = getpass.getuser()
    passwd = getpass.getpass(prompt='password: ')

    engine = PyMySQL()
    engine._init_(host, user, passwd, db, 3306, 'UTF-8')  # host/user/password/database

    welcome()
