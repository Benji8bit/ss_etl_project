-- New script in adb.
-- Date: Jan 21, 2024
-- Time: 12:41:23 AM
-------------------------------

--1. Создание таблицы sales с дистрибуцией по check_nm и check_pos

CREATE TABLE std5_80.sales (
	"date" DATE,
	region VARCHAR(20),
	material VARCHAR(20),
	distr_chan VARCHAR(100),
	quantity INT,
	check_nm VARCHAR(100),
	check_pos varchar(100)
)
WITH (
	appendonly=TRUE,
	orientation=COLUMN,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED BY (check_nm, check_pos)
PARTITION BY RANGE ("date")
(
	START (DATE '1970-01-01') INCLUSIVE 
	END (DATE '2024-01-01') INCLUSIVE
	EVERY (INTERVAL '1 year')
);

--2. Создание таблицы plan с дистрибуцией по региону

CREATE TABLE std5_80.plan (
	"date" DATE,
	region VARCHAR(20),
	matdirec VARCHAR(20),
	quantity INT,
	distr_chan VARCHAR(100)
)
WITH (
	appendonly=TRUE,
	orientation=COLUMN,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED BY (region)
PARTITION BY RANGE("date")
(
	START (DATE '1970-01-01') INCLUSIVE 
	END (DATE '2024-01-01') INCLUSIVE
	EVERY (INTERVAL '1 year')
);

DROP TABLE std5_80.plan;

--3. Создание таблицы price с репликацией (как справочник)

CREATE TABLE std5_80.price (
	material INT4,
	region VARCHAR(4),
	distr_chan VARCHAR(1),
	price INT4	
)
DISTRIBUTED REPLICATED;

DROP TABLE std5_80.price;

--4. Создание таблицы chanel с репликацией (как справочник)

CREATE TABLE std5_80.chanel (
	distr_chan VARCHAR(1),
	txtsh TEXT	
)
DISTRIBUTED REPLICATED;

--5. Создание таблицы product с репликацией (как справочник)

CREATE TABLE std5_80.product (
	material INT,
	asgrp INT,
	brand INT,
	matcateg VARCHAR(4),
	matdirec INT,
	txt TEXT	
)
DISTRIBUTED REPLICATED;

--6. Создание таблицы region с репликацией (как справочник)

CREATE TABLE std5_80.region (
	region VARCHAR(4),
	txt TEXT	
)
DISTRIBUTED REPLICATED;

DROP TABLE std5_80.region;


SELECT *
FROM std5_80.plan
LIMIT 10;