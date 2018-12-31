--create DATABASE fund;
use fund;
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
 
 
DROP TABLE IF  EXISTS fund_managers_chg; 
CREATE TABLE IF NOT EXISTS `fund_managers_chg` (
	`fund_code` varchar(255) NOT NULL COMMENT '基金代码',
	`start_date` varchar(255) DEFAULT NULL COMMENT '起始期',
	`end_date` varchar(255) DEFAULT NULL COMMENT '截止期', 
	`fund_managers` varchar(255) DEFAULT NULL COMMENT '基金经理',
	`term` varchar(255) DEFAULT NULL COMMENT '任职期间',
	`return_rate` varchar(255) DEFAULT NULL COMMENT '任职回报',
	`created_date` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
	`updated_date` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间',
	`data_source` varchar(255) DEFAULT NULL COMMENT '数据来源',
	PRIMARY KEY (`fund_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='基金经理变动一览表';
 
DROP TABLE IF  EXISTS fund_managers_info;
CREATE TABLE IF NOT EXISTS `fund_managers_info` (
	`manager_id` varchar(255) NOT NULL COMMENT '基金经理ID',
	`url` varchar(255) DEFAULT NULL COMMENT '链接',
	`manager_name` varchar(255) DEFAULT NULL COMMENT '基金经理',
	`created_date` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
	`updated_date` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间',
	`data_source` varchar(255) DEFAULT 'eastmoney' COMMENT '数据源',
	PRIMARY KEY (`manager_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='基金经理信息基表'; 
 
DROP TABLE IF  EXISTS fund_nav ;
CREATE TABLE IF NOT EXISTS `fund_nav` (
  `the_date` varchar(255) NOT NULL,
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
 
DROP TABLE IF  EXISTS fund_nav_currency ;
CREATE TABLE IF NOT EXISTS  `fund_nav_currency` (
  `the_date` varchar(255) NOT NULL,
  `fund_code` varchar(255) NOT NULL,
  `profit_per_units` varchar(255) DEFAULT NULL,
  `profit_rate` varchar(255) DEFAULT NULL,
  `buy_state` varchar(255) DEFAULT NULL,
  `sell_state` varchar(255) DEFAULT NULL,
  `div_record` varchar(255) DEFAULT NULL,
 
  `created_date` datetime DEFAULT NULL,
  `updated_date` datetime DEFAULT NULL,
  PRIMARY KEY (`the_date`,`fund_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF  EXISTS fund_managers_his;
CREATE TABLE IF NOT EXISTS `fund_managers_his` (
	`manager_id` varchar(255) NOT NULL COMMENT '基金经理ID',
	`manager_url` varchar(255) DEFAULT NULL COMMENT '链接',
	`manager_name` varchar(255) DEFAULT NULL COMMENT '基金经理名称',
	`fund_code` varchar(255) DEFAULT NULL COMMENT '基金ID',
	`fund_name` varchar(255) DEFAULT NULL COMMENT '基金名称',
	`fund_type` varchar(255) DEFAULT NULL COMMENT '基金类型',
	`fund_scale` varchar(255) DEFAULT NULL COMMENT '基金规模（亿元）',
	`start_date` varchar(255) DEFAULT NULL COMMENT '起始期',
	`end_date` varchar(255) DEFAULT NULL COMMENT '止期',
	`term` varchar(255) DEFAULT NULL COMMENT '任期时长',
	`return_rate` varchar(255) DEFAULT NULL COMMENT '回报率',	
	`updated_date` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间',
	`data_source` varchar(255) DEFAULT 'eastmoney' COMMENT '数据源',
	PRIMARY KEY (`manager_id`,`fund_code`,`start_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='基金经理履历表'; 



