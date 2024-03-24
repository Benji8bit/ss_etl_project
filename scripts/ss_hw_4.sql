-- New script in adb.
-- Date: Jan 25, 2024
-- Time: 2:15:22 AM
-------------------------------


/*
 "Интеграция с внешними системами"
 
Параметры подключения к Postgres:
        Хост          	             Порт   	   База данных    	       Логин // Пароль         
    192.168.214.212     	         5432	         postgres	           intern // intern

1. Необходимо создать внешние таблицы в Greenplum c использованием протокола PXF для доступа к данным следующих таблиц базы Postgres: 
gp.plan 
gp.sales 
2. Необходимо создать внешние таблицы в Greenplum c использованием протокола gpfdist для доступа к данным следующих файлов CSV: 
price 
chanel 
product 
region 
Установщик утилиты "gpfdist" прикреплен ниже.
 
chanel.csv chanel.csv17 January 2024, 13:16
 
greenplum-db-clients-6.20.0-x86_64.msi greenplum-db-clients-6.20.0-x86_64.msi8 May 2023, 12:08
 
price.csv price.csv17 January 2024, 13:16
 
product.csv product.csv17 January 2024, 13:16
 
region.csv region.csv17 January 2024, 13:16
 */


SET SEARCH_PATH TO std5_80;

CREATE EXTERNAL TABLE std5_80.plan_ext ( 
 "date" date , 
 region varchar(20) , 
 matdirec varchar(20) , 
 quantity int4 , 
 distr_chan varchar(100)
) 
LOCATION ('pxf://gp.plan?PROFILE=JDBC&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER=intern&PASS=intern' 
) ON ALL  
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import') 
ENCODING 'UTF8';

DROP EXTERNAL TABLE std5_80.plan_ext;

SELECT *
FROM std5_80.plan_ext
LIMIT 10;

CREATE EXTERNAL TABLE std5_80.sales_ext ( 
    "date" DATE,
	region VARCHAR(20),
	material VARCHAR(20),
	distr_chan VARCHAR(100),
	quantity INT,
	check_nm VARCHAR(100),
	check_pos varchar(100)
) 
LOCATION ('pxf://gp.sales?PROFILE=JDBC&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER=intern&PASS=intern' 
) ON ALL  
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import') 
ENCODING 'UTF8';

SELECT *
FROM std5_80.sales_ext
LIMIT 10;

CREATE EXTERNAL TABLE std5_80.price_ext (
  material int4,
  region varchar(4),
  distr_chan varchar(1),
  price int4
)
LOCATION('gpfdist://172.16.128.118:8080/price.csv') 
ON ALL 
FORMAT 'CSV'(delimiter ';')
ENCODING 'UTF8'
SEGMENT REJECT LIMIT 10 ROWS

SELECT *
FROM std5_80.price_ext
LIMIT 10;

CREATE EXTERNAL TABLE std5_80.chanel_ext (
	distr_chan VARCHAR(1),
	txtsh TEXT
)
LOCATION('gpfdist://172.16.128.118:8080/chanel.csv') 
ON ALL 
FORMAT 'CSV'(delimiter ';')
ENCODING 'UTF8'
SEGMENT REJECT LIMIT 10 ROWS;

SELECT *
FROM std5_80.chanel_ext
LIMIT 10;

CREATE EXTERNAL TABLE std5_80.product_ext (
	material INT,
	asgrp INT,
	brand INT,
	matcateg VARCHAR(4),
	matdirec INT,
	txt TEXT
)
LOCATION('gpfdist://172.16.128.118:8080/product.csv') 
ON ALL 
FORMAT 'CSV'(delimiter ';')
ENCODING 'UTF8'
SEGMENT REJECT LIMIT 10 ROWS;

DROP EXTERNAL TABLE std5_80.product_ext;

SELECT *
FROM std5_80.product_ext
LIMIT 10;

CREATE EXTERNAL TABLE std5_80.region_ext (
	region VARCHAR(4),
	txt TEXT
)
LOCATION('gpfdist://172.16.128.118:8080/region.csv') 
ON ALL 
FORMAT 'CSV'(delimiter ';')
ENCODING 'UTF8'
SEGMENT REJECT LIMIT 10 ROWS;

SELECT *
FROM std5_80.region_ext
LIMIT 10;
