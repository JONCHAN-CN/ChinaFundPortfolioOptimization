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
                    filename='../log/2-cleanFundData.log',
                    filemode='a',
                    datefmt='%Y/%m/%d %H:%M:%S',
                    format='%(levelname)s %(asctime)s %(funcName)s %(lineno)d %(message)s'
                    )
logger = logging.getLogger(__name__)
chlr = logging.StreamHandler() # 输出到控制台的handler
logger.addHandler(chlr)

#def resampleNAV(table,frequency = 'B'):
#    # table.index = table.index.astype('datetime64[D]')
#    # table.drop('the_date',axis =1, inplace= True)         
#    table = table.resample(frequency).asfreq()
#    return table

# 切片数据
def calNAVStat(table):
    dateStat = table.count(axis =1)
    fundStat = table.count()
    col = list([70,80,90,95])
    df = pd.DataFrame(np.zeros([2,4]),columns = col,index = ['dateStat','fundStat'])    
    for i in col:
        df.loc['dateStat',i] = np.percentile(dateStat,i)
        df.loc['fundStat',i] = np.percentile(fundStat,i)
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
        dbTable = pd.read_csv('../out/1.5-%s.csv' %dbTableName,dtype = str)       
        # dbTable.columns = ['fund_code',column,'the_date']
#        #! for first batch of data only
#        dbTable['the_date'] = dbTable['the_date'].str.replace('*','')
#        dbTable['the_date'] = dbTable.apply(lambda x: x['the_date'].split(u'起始时间')[0].strip(), axis = 1)
#        #! upper for first batch of data only
        dbTable['the_date'] = dbTable['the_date'].values.astype('datetime64[D]') 
        dbTable[column] = pd.to_numeric(dbTable[column],errors='coerce')           
        start = time.time()
        dbTable = dbTable.pivot('the_date','fund_code',column)
        dbTable = dbTable.fillna(method = 'ffill').resample(frequency).asfreq()
        end = time.time()
        msg = "Time used to pivot table and resample: "+str(end - start)
        logger.info(msg)
        
        toCSV(dbTable,'../out/2-%s-%s-%s.csv' %(dbTableName,column,frequency))
        msg = str("NAV数据初步处理成功！[%s-%s-%s]"%(dbTableName,column,frequency))      
        logger.info(msg)
        return dbTable          
#        elif table=='fund_nav_currency':
#        # fund_info
#        elif table=='fund_info':
#            dbTable.columns = ['fund_code','fund_name','fund_abbr_name','fund_type','issue_date','establish_date','establish_scale','asset_value','asset_value_date','units','units_date','fund_manager','fund_trustee','funder','total_div','mgt_fee','trust_fee','sale_fee','buy_fee','buy_fee2','benchmark','underlying','data_source','created_date','updated_date','created_by','updated_by']
#            dbTable['div_times'] = dbTable.apply(lambda x: x['total_div'].split(u'（')[1].strip(u'次）'), axis = 1)
#            dbTable['total_div'] = dbTable.apply(lambda x: x['total_div'].split(u'元（')[0].strip(u'每份累计'), axis = 1)
#            toCSV(dbTable,exportDir+'%s.csv'%table)
#            # print('Done here!!')
#        elif table=='fund_managers_chg':
#            #TODO
#            print('TODO!!')
#        elif table=='fund_managers_info':
#            #TODO
#            print('TODO!!')
#        else:  
#            pass
    except Exception as e:
        msg = str("NAV数据初步处理失败！\n%s"%e)        
        logger.exception(msg)         
        return 0
    
### Main ######################################################################
def main():
    # tableName,columnName,frequency
    dbTable = processNAV('fund_nav','nav','B')    
    B4_dateStat,B4_fundStat,B4_dbStat = calNAVStat(dbTable)
    dbRe = dbTable.resample('BM').asfreq() # Table, Frequency  here
    AFT_dateStat,AFT_fundStat,AFT_dbStat = calNAVStat(dbRe)
    # precise cor
    start = time.time()
    corTotal = comb(dbRe.shape[1],2)
    count = 0
    corMat = pd.DataFrame(np.zeros(shape = [dbRe.shape[1],dbRe.shape[1]]),columns = dbRe.columns,index = dbRe.columns)
    for a, b in itertools.combinations(dbRe.columns, 2):
        preCor = dbRe[[a,b]].fillna(method = 'ffill')
        # preCor = preCor.fillna(method = 'ffill')
        preCor = preCor[preCor.count(axis =1)==2]
        # preCor = preCor.T
        corMat.loc[a,b] = np.cov(preCor.T)[1,0]
        corMat.loc[b,a] = corMat.loc[a,b]
        count=count+1
        if count%1000 ==0:
            print("Done calculated [%d/ %f%%]..."%(count,count*100/corTotal))
    for c in itertools.combinations(dbRe.columns, 1):
        preCor = dbRe[c].fillna(method = 'ffill').dropna()
        corMat.loc[c,c] = np.var(preCor.T)

    end = time.time()
    msg = "Time used : "+str(end - start)
    logger.info(msg)
    #TODO: fund_info
    #TODO: manager_info/ manager_chg

       
if __name__ == "__main__":
    main()  
      