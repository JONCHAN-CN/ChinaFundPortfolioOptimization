# Fund Portfolio

## Database Mgmt
### Database Generating
- connect to database -> [pass.txt](./mgmt/pass.txt)
- create table -> [createStatTable.sql](./mgmt/database/createStatTable.sql)
- import data(IP,acct) -> [importDB.py](./mgmt/database/importDB.py)
    - [IP file](./proxy/ips-2020-03-16.txt)

### Database Processing
- TODO

## Data Scraping
### Src
- core of daily run -> [beTrue.py](./beTrue.py)
- control multiple instances to run core -> [beControl.py](./beControl.py)
- **ONE KEY**  init control multiple instances to run core -> [beControl.bat](./beControl.bat)
- [beUpdateConfig.py](./beUpdateConfig.py)


## Data Processing 



## Portfolio Backtest
### 1. Backtest
- select a `**backtest date**, if a backtest date is 
    - **10 days** within the customize backtest date
    - possessed **>=90%** of fund nav data of that date
    - is a **weekday**; if NOT, date back the latest
- get **all effective portfoilo** and **all relevant nav** (all train date and backtest date nav)
- merge **portfolio with nav of train and backtest date**
- calculate
    - **period days** (btw train date and backtest date)
    - **expected return** according to **period months** and **returns** (from portfolio)
    - **actual return** based on weighted **[(backtest_add_nav - train_add_nav)/train_nav]**
- judge a portfolio
    - **TOP** if its **actual return per month > threshold** (60% qt, all result taken into account) and its **actual return > 0**
    - **ACHIEVED** if its **actual return/expected return> coefficient** (60% , single port params) and its **actual return > 0**
    - **EXPIRED** if **NOT TOP OR ACHIEVED**
### 2. Plot

### 3. Most Weighted
