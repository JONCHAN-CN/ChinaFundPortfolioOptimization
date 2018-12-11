select fund_code, count(*)
from (select distinct * from fund_nav) t
group by fund_code
INTO OUTFILE '1-fundCheckByDB.csv'
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n';