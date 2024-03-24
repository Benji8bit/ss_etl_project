-- New script in INTERN Clickhouse 206.
-- Date: Feb 23, 2024
-- Time: 12:44:20 AM
--------------------------------------------------------------------------------

/*
1. Создайте в своей базе данных интеграционную таблицу ch_stores_mart_ext 
для доступа к данным витрины v_stores_mart в системе Greenplum.
*/

CREATE TABLE std5_80.ch_stores_mart_ext
(
    `plant` String,
    `calday` Date,
    `revenue` Decimal(9, 2),
    `coupon_disc` Decimal(9, 2),
    `sold_qty` Int32,
    `bills_qty` Int32,
    `traffic` Int32,
    `promo_qty` Int32
)
ENGINE = PostgreSQL('192.168.214.203:5432',
 'adb',
 'stores_mart',
 'std5_80',
 '9HjeACizqp8T',
 'std5_80');

--------------------------------------------------------------------------------
 
DROP TABLE std5_80.ch_stores_mart_ext --ON CLUSTER default_cluster;

SELECT *
FROM std5_80.ch_stores_mart_ext
ORDER BY plant, calday;

--------------------------------------------------------------------------------

/*
2. Создайте словари для доступа к данным таблиц системы Greenplum:
*/

CREATE DICTIONARY std5_80.ch_stores_dict ON CLUSTER default_cluster
(
    `plant` char(4),
    `txt` varchar
)
PRIMARY KEY plant
SOURCE(POSTGRESQL(PORT '5432' HOST '192.168.214.203' USER 'std5_80' PASSWORD '9HjeACizqp8T' DB 'adb' TABLE 'std5_80.stores'))
LIFETIME(MIN 0 MAX 3600)
LAYOUT(COMPLEX_KEY_HASHED);

DROP DICTIONARY IF EXISTS std5_80.ch_stores_dict ON CLUSTER default_cluster;

CREATE DICTIONARY std5_80.ch_promos_dict ON CLUSTER default_cluster
(
    `promo_id` String,
	`txt` String,
	`type` Int32,
	`material` Int32,
	`discount` Int32
)
PRIMARY KEY promo_id
SOURCE(POSTGRESQL(PORT '5432' HOST '192.168.214.203' USER 'std5_80' PASSWORD '9HjeACizqp8T' DB 'adb' TABLE 'std5_80.promos'))
LIFETIME(MIN 0 MAX 3600)
LAYOUT(COMPLEX_KEY_HASHED);

DROP DICTIONARY IF EXISTS std5_80.ch_promos_dict ON CLUSTER default_cluster;
--------------------------------------------------------------------------------

/*
3. Создайте реплицированные таблицы ch_stores_mart на всех хостах кластера. 
Создайте распределённую таблицу ch_stores_mart_distr, 
выбрав для неё корректный ключ шардирования. 
Вставьте в неё все записи из таблицы  ch_stores_mart_ext.*/

CREATE TABLE std5_80.ch_stores_mart ON CLUSTER default_cluster
(
    `plant` String,
    `calday` Date,
    `revenue` Decimal(9, 2),
    `coupon_disc` Decimal(9, 2),
    `sold_qty` Int32,
    `bills_qty` Int32,
    `traffic` Int32,
    `promo_qty` Int32
)
ENGINE = ReplicatedMergeTree('/click/std5_80.ch_stores_mart/{shard}',
 '{replica}')
PARTITION BY toYYYYMM(calday)
ORDER BY (plant, calday)
SETTINGS index_granularity = 1024;

DROP TABLE IF EXISTS std5_80.ch_stores_mart ON CLUSTER default_cluster;

ALTER TABLE std5_80.ch_stores_mart ON CLUSTER default_cluster 
DELETE WHERE calday BETWEEN '2021-01-01' and '2021-02-28';

INSERT INTO std5_80.ch_stores_mart 
SELECT * FROM std5_80.ch_stores_mart_ext 
WHERE calday BETWEEN '2021-01-01' and '2021-02-28';

SELECT *
FROM std5_80.ch_stores_mart
ORDER BY plant, calday;


--------------------------------------------------------------------------------

CREATE TABLE std5_80.ch_stores_mart_distr  ON CLUSTER default_cluster
(
    `plant` String,
    `calday` Date,
    `revenue` Decimal(9, 2),
    `coupon_disc` Decimal(9, 2),
    `sold_qty` Int32,
    `bills_qty` Int32,
    `traffic` Int32,
    `promo_qty` Int32
)
ENGINE = Distributed('default_cluster',
 'std5_80',
 'ch_stores_mart',
 cityHash64(plant));

DROP TABLE IF EXISTS std5_80.ch_stores_mart_distr ON CLUSTER default_cluster;

ALTER TABLE std5_80.ch_stores_mart_ext ON CLUSTER default_cluster 
DELETE WHERE calday BETWEEN '2021-01-01' and '2021-02-28';

INSERT INTO std5_80.ch_stores_mart_distr 
SELECT * FROM std5_80.ch_stores_mart_ext 
WHERE calday BETWEEN '2021-01-01' and '2021-02-28';

SELECT *
FROM std5_80.ch_stores_mart_distr
ORDER BY plant, calday;

--------------------------------------------------------------------------------