# 基金（中国市场）投资组合优化
基于[Harry  Markowitz](https://en.m.wikipedia.org/wiki/Harry_Markowitz)的[投资组合理论](https://wiki.mbalib.com/wiki/%E6%8A%95%E8%B5%84%E7%BB%84%E5%90%88%E7%90%86%E8%AE%BA)及中国市场基金数据的Python 实现。

A Python implementation of [Harry  Markowitz](https://en.m.wikipedia.org/wiki/Harry_Markowitz)'s [Portfolio Theory](https://wiki.mbalib.com/wiki/%E6%8A%95%E8%B5%84%E7%BB%84%E5%90%88%E7%90%86%E8%AE%BA) with fund data of China market.

**说明/Notes**

本项目部分代码源自网络，经修改后达到项目目的。原代码的作者与出处细列如下。

Part of code in the project was from Internet, and been fitted into this project. Cited code, authors and original posts are listed below.

[【Python 量化投资系列】python3 获取基金及历史净值数据](https://blog.csdn.net/yuzhucu/article/details/55261024) by [yuzhucu](https://blog.csdn.net/yuzhucu)

[The Efficient Frontier: Markowitz portfolio optimization in Python](https://blog.quantopian.com/markowitz-portfolio-optimization-2/) by [Thomas ](https://blog.quantopian.com/author/twiecki/)

## 环境/ Environment
Windows 10/ Python 3.6/ MySQL Community 8.0

## 文件说明/ File Structure
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

## 建库表/ Create Tables in MySQL
创建项目所需表格：

 - `fund_info` -> `基金基本信息表`
 - `fund_managers_chg` ->`基金经理变动一览表`
 - `fund_managers_info` ->`基金经理信息基表`
  - `fund_managers_his` -> `基金经理履历表`
- `fund_nav` -> `非货币型基金净值表`
 - `fund_nav_currency` -> `货币型基金净值表`
 - `fund_nav_quantity` -> `基金净值数量表`
 
## 爬取基金数据/Scapy Fund Data from Internet 
**功能/Function**
- `Update all FUND INFO & MANAGER INFO` -> `更新全量基金与经理信息（除净值与经理履历）`
- `Update all NAV INFO` -> `更新全量基金净值信息`
- `Update last 49 days NAV INFO` -> `更新新增（49天）基金净值信息`
- `Update all MANAGER HISTORY` -> `更新全量基金经理履历信息`
- `Just Check NAV INFO` -> `检查网络上与数据库上的基金净值数量`
- `Export all TABLES` -> `导出所有表为 *.csv`
- `Update ONE FUND ` -> `更新一支基金的所有信息`

**特性/Feature**
- 多线程爬取数据，单线程导入数据/ Multi-threading for data scraping, single threading for data import
- 绕过东方财富网的反爬机制（现在不能一次爬完所有数据了...）/Bypassed the anti-scraping mechanism of the site

## 数据处理/Data Processing
**净值处理/Processing NAV**
- 调整日期格式/Adjust date format
- pivot 成宽表/Pivot data
- 统计数据情况/ Calculate data info

**经理评分/Scoring Managers**
- 修整不规范数据/Trim unformatted data
- 格式化时间字段/Format time-related columns
- 计算**经理资历**/Calculate **managers' total term**
-  计算**经理在行时间与比率**/Calculate **managers' cumulative on-duty  term & percentage**
-  计算**基金年均收益率**/Calculate **fund based annual return rate**
- 计算**经理年均收益评分**/Calculate **manager based annual return score**

-- $\frac{r*w*\sum_0^nt}{\sum_0^nt}$

--`r` for `manager based annual return rate`

--`w` for `weight based on year of served term`

--`t` for `duration of served term` 

## Modeling
**筛选基金/Filtering Fund**

由于算法通过组合基金、采样列举计算凸优化的方法进行计算，数据量大时运行缓慢。这里利用`数据处理`中生成的`4个指标`（上文粗体）筛选基金，减少后续计算量。

Due to large demand of computational capacity to finish convex optimizaton of the combination of funds, we fisrt filter some funds based on the `4 metrics` (bold font mentioned above)computed in `Data Processing` to reduce computation in next step.

**切分数据/Spliting Data**

切分数据为`训练组`和`回测组`（理论上，只取最后一周期）。

Spliting Data into `train set` and `backtest set`(1 period of data, theoratically)

**计算投资组合**

计算`训练组`投资组合，对每一个基金组合，输出三类组合/ For each combination of fund in`train set`,output 3 type of  portfolio:
- 最大夏普比率组合/Portfolio w/ maximum Sharpe Ratio
- 最小风险组合/Portfolio w/ minium Risk 
- 默认组合/ Default portfolio


每一个组合包含/ Each portfolio contains:

- `基金组合及比例`/`fund portfolio`
- `预期收益率`/`expected return rate`
- `预期风险`/`expected risk`
- `预期夏普比率`/`expected Sharpe Ratio`
- `类型`/`type of portfolio`

**回测/Backtest**

抽取结果表中夏普比率排序前10%的组合，如下一期`真实收益率` 达到`预期收益率`的80%或以上，认为命中。
Extract top 10% portfolio according to Sharpe Ratio for backtest. if `actual return rate` reaches 80% of `expected return rate`, this portfolio stands.

#TODO backtest...
> Written with [StackEdit](https://stackedit.io/).
