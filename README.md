# 基金（中国市场）投资组合优化
基于[Harry  Markowitz](https://en.m.wikipedia.org/wiki/Harry_Markowitz)的[投资组合理论](https://wiki.mbalib.com/wiki/%E6%8A%95%E8%B5%84%E7%BB%84%E5%90%88%E7%90%86%E8%AE%BA)及中国市场基金数据的Python 实现。

A Python implementation of [Harry  Markowitz](https://en.m.wikipedia.org/wiki/Harry_Markowitz)'s [Portfolio Theory](https://wiki.mbalib.com/wiki/%E6%8A%95%E8%B5%84%E7%BB%84%E5%90%88%E7%90%86%E8%AE%BA) with fund data of China market.

**说明/Notes**
本项目部分代码源自网络，经修改后达到项目目的。原代码的作者与出处细列如下。
Part of code in the project was from Internet, and been fitted into this project. Cited code, authors and original posts are listed below.

[【Python 量化投资系列】python3 获取基金及历史净值数据](https://blog.csdn.net/yuzhucu/article/details/55261024) by [yuzhucu](https://blog.csdn.net/yuzhucu)
[The Efficient Frontier: Markowitz portfolio optimization in Python](https://blog.quantopian.com/markowitz-portfolio-optimization-2/) by [Thomas ](https://blog.quantopian.com/author/twiecki/)

## 环境
Windows 10/ Python 3.6/ MySQL Community 8.0

## 文件说明
├─dep
--│1-fundCode&Name.csv  -> code & name of funds in China market
--│1-fundCode.csv
├─log   -> log folder, will be created by script if not exist
├─out ->  output folder, will be created by script if not exist
└─src
--│ 0-createFundTable.sql -> script to create tables in MySQL
--│ 1-generateFundData.py -> script to scrapy fund data from [eastmoney.com](http://www.eastmoney.com/)
--│ 2-generateFundPortfolio.py -> script to build model
--│ equirements.txt
--└─utils
----│ DataProcess.py -> script to process data before modeling 
----│ PyMySQL.py

## Create Tables in MySQL
 `fund_info` -> `基金基本信息表`
 `fund_managers_chg` ->`基金经理变动一览表`
 `fund_managers_info` ->`基金经理信息基表`
  `fund_managers_his` -> `基金经理履历表`
`fund_nav`-> `非货币型基金净值表`
 `fund_nav_currency` -> `货币型基金净值表`
 `fund_nav_quantity` -> `基金净值数量表`
## Scapy Fund Data from Internet 
1 here
## Data Processing
utils.data process here
## Modeling
2 here

> Written with [StackEdit](https://stackedit.io/).
