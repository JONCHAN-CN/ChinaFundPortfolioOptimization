# -*- coding: utf-8 -*-
"""
Created on Mon Nov  5 19:02:10 2018
@author: JON7390
交易一期(基金)数据处理
"""
import time  
import os  
import pandas as pd  
import logging
import numpy as np
import itertools
from scipy.special import comb

logging.basicConfig(level=logging.INFO,
                    filename='./log/2-cleanFundData.log',
                    filemode='a',
                    datefmt='%Y/%m/%d %H:%M:%S',
                    format='%(levelname)s %(asctime)s %(funcName)s %(lineno)d %(message)s'
                    )
logger = logging.getLogger(__name__)
# chlr = logging.StreamHandler() # 输出到控制台的handler
logger.addHandler(logging.StreamHandler())

# NAV切片数据
def calNAVStat(table):
    dateStat = table.count(axis =1)
    fundStat = table.count()
    col = list([50,60,70,80,90,95])
    df = pd.DataFrame(np.zeros([2,6]),columns = col,index = ['dateStat','fundStat'])
    for i in col:
        df.loc['dateStat',i] = np.percentile(dateStat,i)
        df.loc['fundStat',i] = np.percentile(fundStat,i)

    # Create a Pandas Excel writer using XlsxWriter as the engine.
    writer = pd.ExcelWriter('./out/2-NAVStat.xlsx', engine='xlsxwriter')
    # Convert the dataframe to an XlsxWriter Excel object.
    dateStat.to_excel(writer, sheet_name='dateStat')
    fundStat.to_excel(writer, sheet_name='fundStat')
    df.to_excel(writer, sheet_name='percentileStat')
    # Close the Pandas Excel writer and output the Excel file.
    writer.save()
    return  dateStat,fundStat,df

def toCSV(dbTable,fullExportPath):
    try:
        dbTable.to_csv(fullExportPath)
        msg = "Done output to CSV file: %s"%fullExportPath
        logger.info(msg)    
    except Exception as e:
        msg = str("导出为CSV失败！\n%s" %e)        
        logger.exception(msg)           

def processNAV(dbTableName,column,frequency = 'B'):
    try:
        dbTable = pd.read_csv('./out/1.5-%s.csv' %dbTableName,dtype = str)
        dbTable['the_date'] = dbTable['the_date'].values.astype('datetime64[D]')
        dbTable[column] = pd.to_numeric(dbTable[column],errors='coerce')           
        start = time.time()
        dbTable = dbTable.pivot('the_date','fund_code',column)
        dbTable = dbTable.fillna(method = 'ffill').resample(frequency).asfreq()
        end = time.time()
        msg = "Time used to pivot table and resample: "+str(end - start)
        logger.info(msg)
        toCSV(dbTable,'./out/2-%s-%s-%s.csv' %(dbTableName,column,frequency))
        msg = str("NAV数据初步处理成功！[%s-%s-%s]"%(dbTableName,column,frequency))      
        logger.info(msg)
        return dbTable
    except Exception as e:
        msg = str("NAV数据初步处理失败！\n%s"%e)        
        logger.exception(msg)         
        return 0
    
### Main ######################################################################
def main():
    ### NAV
    # tableName,columnName,frequency
    dbTable = processNAV('fund_nav','nav','B')
    dateStat,fundStat,dbStat = calNAVStat(dbTable)
    # dbRe = dbTable.resample('BM').asfreq()
    # AFT_dateStat,AFT_fundStat,AFT_dbStat = calNAVStat(dbRe)

    #TODO: fund_info
    #TODO: manager_info/ manager_chg

if __name__ == "__main__":
    main()