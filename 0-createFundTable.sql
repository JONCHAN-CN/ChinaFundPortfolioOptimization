--create DATABASE fund;
--use fund;


-- fund info
DROP TABLE IF  EXISTS fund_info ;
CREATE TABLE IF NOT EXISTS `fund_info` (
  `fund_code` varchar(255) NOT NULL COMMENT '基金代码',
  `fund_name` varchar(255) DEFAULT NULL COMMENT '基金全称',
  `fund_abbr_name` varchar(255) DEFAULT NULL COMMENT '基金简称',
  `fund_type` varchar(255) DEFAULT NULL COMMENT '基金类型',
  `issue_date` varchar(255) DEFAULT NULL COMMENT '发行日期',
  `establish_date` varchar(255) DEFAULT NULL COMMENT '成立日期',
  `establish_scale` varchar(255) DEFAULT NULL COMMENT '成立日期规模',
  `asset_value` varchar(255) DEFAULT NULL COMMENT '最新资产规模',
  `asset_value_date` varchar(255) DEFAULT NULL COMMENT '最新资产规模日期',
  `units` varchar(255) DEFAULT NULL COMMENT '最新份额规模',
  `units_date` varchar(255) DEFAULT NULL COMMENT '最新份额规模日期',
  `fund_manager` varchar(255) DEFAULT NULL COMMENT '基金管理人',
  `fund_trustee` varchar(255) DEFAULT NULL COMMENT '基金托管人',
  `funder` varchar(255) DEFAULT NULL COMMENT '基金经理人',
  `total_div` varchar(255) DEFAULT NULL COMMENT '成立来分红',
  `mgt_fee` varchar(255) DEFAULT NULL COMMENT '管理费率',
  `trust_fee` varchar(255) DEFAULT NULL COMMENT '托管费率',
  `sale_fee` varchar(255) DEFAULT NULL COMMENT '销售服务费率',
  `buy_fee` varchar(255) DEFAULT NULL COMMENT '最高认购费率',
  `buy_fee2` varchar(255) DEFAULT NULL COMMENT '最高申购费率',
  `benchmark` varchar(1000) DEFAULT NULL COMMENT '业绩比较基准',
  `underlying` varchar(500) DEFAULT NULL COMMENT '跟踪标的',
  `data_source` varchar(255) DEFAULT 'eastmoney' COMMENT '数据来源',
  `created_date` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_date` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间',
  `created_by` varchar(255) DEFAULT 'eastmoney' COMMENT '创建人',
  `updated_by` varchar(255) DEFAULT 'eastmoney' COMMENT '更新人',
  PRIMARY KEY (`fund_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='基金基本信息表';
 

-- manager
DROP TABLE IF  EXISTS fund_managers_chg; 
CREATE TABLE IF NOT EXISTS `fund_managers_chg` (
	`fund_code` varchar(255) NOT NULL COMMENT '基金代码',
	`start_date` varchar(255) NOT NULL COMMENT '起始期',
	`end_date` varchar(255) DEFAULT NULL COMMENT '截止期', 
	`fund_managers` varchar(255) DEFAULT NULL COMMENT '基金经理',
	`term` varchar(255) DEFAULT NULL COMMENT '任职期间',
	`return_rate` varchar(255) DEFAULT NULL COMMENT '任职回报',
	`created_date` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
	`updated_date` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间',
	`data_source` varchar(255) DEFAULT NULL COMMENT '数据来源',
	PRIMARY KEY (`fund_code`,`start_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='基金经理变动一览表';
 
 
DROP TABLE IF  EXISTS managers_info;
CREATE TABLE IF NOT EXISTS `managers_info` (
	`manager_id` varchar(255) NOT NULL COMMENT '基金经理ID',
	`url` varchar(255) DEFAULT NULL COMMENT '链接',
	`manager_name` varchar(255) DEFAULT NULL COMMENT '基金经理',
	`created_date` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
	`updated_date` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间',
	`data_source` varchar(255) DEFAULT 'eastmoney' COMMENT '数据源',
	PRIMARY KEY (`manager_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='基金经理信息基表'; 
 
 
 DROP TABLE IF  EXISTS managers_his;
CREATE TABLE IF NOT EXISTS `managers_his` (
	`manager_id` varchar(255) NOT NULL COMMENT '基金经理ID',
	`manager_url` varchar(255) DEFAULT NULL COMMENT '链接',
	`manager_name` varchar(255) DEFAULT NULL COMMENT '基金经理名称',
	`cum_on_duty_term` varchar(255) NOT NULL COMMENT '累计任职时间',
	`fund_code` varchar(255) NOT NULL COMMENT '基金ID',
	`fund_name` varchar(255) DEFAULT NULL COMMENT '基金名称',
	`fund_type` varchar(255) DEFAULT NULL COMMENT '基金类型',
	`fund_scale` varchar(255) DEFAULT NULL COMMENT '基金规模（亿元）',
	`start_date` varchar(255) NOT NULL COMMENT '起始期',
	`end_date` varchar(255) DEFAULT NULL COMMENT '止期',
	`term` varchar(255) DEFAULT NULL COMMENT '任期时长',
	`return_rate` varchar(255) DEFAULT NULL COMMENT '回报率',	
	`updated_date` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间',
	`data_source` varchar(255) DEFAULT 'eastmoney' COMMENT '数据源',
	PRIMARY KEY (`manager_id`,`fund_code`,`start_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='基金经理履历表'; 


-- nav
DROP TABLE IF  EXISTS nav ;
CREATE TABLE IF NOT EXISTS `nav` (
  `date` varchar(255) NOT NULL,
  `nav` varchar(255) DEFAULT NULL,
  `add_nav` varchar(255) DEFAULT NULL,
  `nav_chg_rate` varchar(255) DEFAULT NULL,
  `buy_state` varchar(255) DEFAULT NULL,
  `sell_state` varchar(255) DEFAULT NULL,
  `div_record` varchar(255) DEFAULT NULL,
  `fund_code` varchar(255) NOT NULL,
  `created_date` datetime DEFAULT NULL,
  `updated_date` datetime DEFAULT NULL,
   PRIMARY KEY (`the_date`,`fund_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='非货币基金净值表';
 
 
DROP TABLE IF  EXISTS nav_currency ;
CREATE TABLE IF NOT EXISTS  `nav_currency` (
  `date` varchar(255) NOT NULL,
  `fund_code` varchar(255) NOT NULL,
  `profit_per_units` varchar(255) DEFAULT NULL,
  `profit_rate` varchar(255) DEFAULT NULL,
  `buy_state` varchar(255) DEFAULT NULL,
  `sell_state` varchar(255) DEFAULT NULL,
  `div_record` varchar(255) DEFAULT NULL,
 
  `created_date` datetime DEFAULT NULL,
  `updated_date` datetime DEFAULT NULL,
  PRIMARY KEY (`the_date`,`fund_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='货币基金净值表';


DROP TABLE IF  EXISTS nav_quantity ;
CREATE TABLE IF NOT EXISTS `nav_quantity` (
  `fund_code` varchar(255) NOT NULL,
  `quantity` varchar(255) DEFAULT NULL,
  `updated_date` datetime DEFAULT NULL,
   PRIMARY KEY (`fund_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='基金净值数量表'; 


-- model TODO update acct to database.ddl
CREATE TABLE `fund_portfolio_3` (
  `id` int(15) NOT NULL AUTO_INCREMENT,
  `fundCode_0` varchar(255) NOT NULL,
  `fundCode_1` varchar(255) NOT NULL,
  `fundCode_2` varchar(255) NOT NULL,
  `portfolio_0` float NOT NULL,
  `portfolio_1` float NOT NULL,
  `portfolio_2` float NOT NULL,
  `returns` float NOT NULL,
  `risks` float NOT NULL,
  `sharpeRatio` float NOT NULL,
  `label` varchar(255) DEFAULT NULL,
  `train_date` datetime NOT NULL,
  `expire_date` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='基金配资表（3位）'


CREATE TABLE `fund_backtest_3` (
  `id` int(15) NOT NULL,
  `est_returns` float NOT NULL,
  `act_returns` float NOT NULL,
  `time_delay` int(11) NOT NULL,
  `backtest_date` datetime NOT NULL,
  `expire_flag` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`id`,`backtest_date`),
  CONSTRAINT `fund_backtest_3_ibfk_1` FOREIGN KEY (`id`) REFERENCES `fund_portfolio_3` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='基金回测表（3位）'

CREATE TABLE `params` (
  `batchid` int(15) NOT NULL,
  `risk_free` float NOT NULL,
  `fit_frequency` varchar(255) NOT NULL,
  `portfolio_nbr` int(11) NOT NULL,
  `annual_return_score` float NOT NULL,
  `cum_on_duty_term_pct` float NOT NULL,
  `annual_return_fund` float NOT NULL,
  `term` float NOT NULL,
  `weighted_annual_return_score` float NOT NULL,
  `mode` varchar(255) NOT NULL,
  `date` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '入库时间',
  `expire_date` datetime DEFAULT NULL,
  PRIMARY KEY (`batchid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='参数表'


CREATE TABLE `params_benchmark` (
  `batchid` int(15) NOT NULL,
  `backtest_date` datetime NOT NULL,
  `avg_exp_return` float NOT NULL COMMENT '平均非负期望回报',
  `avg_act_return` float NOT NULL COMMENT '平均非负实际回报',
  `pct` float NOT NULL COMMENT '实际回报/期望回报',
  PRIMARY KEY (`batchid`),
  CONSTRAINT `params_benchmark_ibfk_1` FOREIGN KEY (`batchid`) REFERENCES `params` (`batchid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='参数有效率表'





-- side to asist
replace into nav select * from nav_slave;
replace into nav_currency select * from nav_currency_slave;truncate table nav_currency_slave;truncate table nav_slave;
select fund_code, count(*)from nav_slave group by fund_code;select * from nav_slave;select count(*)from nav_slave where
fund_code = 501053;select fund_code, count(*)from nav_currency_slave group by fund_code;select * from nav_currency_slave
;select fund_code, count(*)from nav group by fund_code;select count(*)from nav where fund_code = 671030;select * from
nav where fund_code = 005784;

select * from fund_info where fund_type ='货币型';

show global variables like '%timeout%';
set global net_read_timeout =100;
show global variables like '%packet%';
set global net_read_timeout =256*1024*1024;

SELECT *
FROM fund.nav
where nav != add_nav
and fund_code = 202001;