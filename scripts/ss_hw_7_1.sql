-- New script in 192.168.214.206.
-- Date: Feb 8, 2024
-- Time: 3:02:11 AM
-------------------------------

/*Параметры подключения к Clickhouse:

                    Хост	          Порт	                  Логин // Пароль                
          192.168.214.206          	          8123    username // password          
          192.168.214.209	          8123	          username // password
          192.168.214.210	          8123	          username // password
          192.168.214.211	          8123	          username // password
*/
/*
1. Создайте базу данных std<номер пользователя> на 206 хосте.
*/

CREATE DATABASE std5_80 ON CLUSTER default_cluster;

DROP DATABASE IF EXISTS std5_80 ON CLUSTER default_cluster;

/*
2. Создайте в своей базе данных интеграционную таблицу ch_plan_fact_ext 
для доступа к данным витрины plan_fact_<YYYYMM> в системе Greenplum.
*/

CREATE TABLE std5_80.ch_plan_fact_ext --ON CLUSTER default_cluster
(
    `region` String,    
    `matdirec` String,
    `distr_chan` String,    
    `plan_qty` Int32,
    `fact_qty` Int32,
    `sales_percent` Decimal(8, 2),
    `bestseller` Int32    
)
ENGINE = PostgreSQL('192.168.214.203:5432',
 'adb',
 'plan_fact_202103',
 'std5_80',
 '9HjeACizqp8T',
 'std5_80');

SELECT *
FROM std5_80.ch_plan_fact_ext;

--------------------------------------------------------------------------------

DROP TABLE std5_80.ch_plan_fact_ext --ON CLUSTER default_cluster;

SELECT *
FROM std5_80.ch_plan_fact_ext;

--------------------------------------------------------------------------------


/*
3. Создайте следующие словари для доступа к данным таблиц системы Greenplum:

ch_price_dict
ch_chanel_dict
ch_product_dict
ch_region_dict
*/

CREATE DICTIONARY std5_80.ch_price_dict ON CLUSTER default_cluster
(
    `material` String,
    `region` String,
    `distr_chan` String,
    `price` Decimal(8, 2)
)
PRIMARY KEY material
SOURCE(POSTGRESQL(PORT '5432' HOST '192.168.214.203' USER 'std5_80' PASSWORD '9HjeACizqp8T' 
				  DB 'adb' TABLE 'std5_80.price'))
LIFETIME(MIN 0 MAX 3600)
LAYOUT(COMPLEX_KEY_HASHED());

DROP DICTIONARY std5_80.ch_price_dict ON CLUSTER default_cluster;

SELECT *
FROM std5_80.ch_price_dict;
--------------------------------------------------------------------------------------------

CREATE DICTIONARY std5_80.ch_chanel_dict ON CLUSTER default_cluster
(
    `distr_chan` String,
    `txtsh` String
)
PRIMARY KEY distr_chan
SOURCE(POSTGRESQL(PORT '5432' HOST '192.168.214.203' USER 'std5_80' PASSWORD '9HjeACizqp8T' 
				  DB 'adb' TABLE 'std5_80.chanel'))
LIFETIME(MIN 0 MAX 3600)
LAYOUT(COMPLEX_KEY_HASHED())
COMMENT 'Chanel';

DROP DICTIONARY std5_80.ch_chanel_dict;

--------------------------------------------------------------------------------------------

CREATE DICTIONARY std5_80.ch_product_dict  ON CLUSTER default_cluster
(
    `material` String,
    `asgrp` String,
    `brand` String,
    `matcateg` String,
    `matdirec` String,
    `txt` String
)
PRIMARY KEY material
SOURCE(POSTGRESQL(PORT '5432' HOST '192.168.214.203' USER 'std5_80' PASSWORD '9HjeACizqp8T' 
				  DB 'adb' TABLE 'std5_80.product'))
LIFETIME(MIN 0 MAX 3600)
LAYOUT(COMPLEX_KEY_HASHED());

DROP DICTIONARY std5_80.ch_product_dict;

SELECT *
FROM std5_80.ch_product_dict
ORDER BY matdirec DESC ;

--------------------------------------------------------------------------------------------

CREATE DICTIONARY std5_80.ch_region_dict ON CLUSTER default_cluster
(
    `region` String,
    `txt` String
)
PRIMARY KEY region
SOURCE(POSTGRESQL(PORT '5432' HOST '192.168.214.203' USER 'std5_80' PASSWORD '9HjeACizqp8T' 
				  DB 'adb' TABLE 'std5_80.region'))
LIFETIME(MIN 0 MAX 3600)
LAYOUT(COMPLEX_KEY_HASHED());

DROP DICTIONARY std5_80.ch_region_dict;

--------------------------------------------------------------------------------------------

/*
4. Создайте реплицированные таблицы ch_plan_fact на всех хостах кластера. 
Создайте распределённую таблицу ch_plan_fact_distr, 
выбрав для неё корректный ключ шардирования. 
Вставьте в неё все записи из таблицы  ch_plan_fact_ext.*/

CREATE TABLE std5_80.ch_plan_fact ON CLUSTER default_cluster
(
    `region` String,    
    `matdirec` String,
    `distr_chan` String,    
    `plan_qty` Int32,
    `fact_qty` Int32,
    `sales_percent` Decimal(8, 2),
    `bestseller` Int32    
)
ENGINE = ReplicatedMergeTree('/click/std5_80/ch_plan_fact/{shard}',
 '{replica}')
ORDER BY region
SETTINGS index_granularity = 16384;  -- defult value = 8192


DROP TABLE IF EXISTS std5_80.ch_plan_fact ON CLUSTER default_cluster;

SELECT *
FROM std5_80.ch_plan_fact;

--------------------------------------------------------------------------------------------

CREATE TABLE std5_80.ch_plan_fact_distr ON CLUSTER default_cluster
(
    `region` String,    
    `matdirec` String,
    `distr_chan` String,    
    `plan_qty` Int32,
    `fact_qty` Int32,
    `sales_percent` Decimal(19, 4),
    `bestseller` Int32    
)
ENGINE = Distributed('default_cluster',
 'std5_80',
 'ch_plan_fact',
 cityHash64(region));

DROP TABLE IF EXISTS std5_80.ch_plan_fact_distr ON CLUSTER default_cluster;

INSERT INTO std5_80.ch_plan_fact_distr
SELECT * FROM std5_80.ch_plan_fact_ext;

SELECT *
FROM std5_80.ch_plan_fact_distr;
 
--------------------------------------------------------------------------------------------

SELECT *
FROM system.macros;