-- New script in adb.
-- Date: Jan 25, 2024
-- Time: 2:15:22 AM
-------------------------------

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

/*
 PXF CONNECTION STRING: pxf://gp.plan?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER=intern&PASS=intern
EXTERNAL TABLE IS: CREATE EXTERNAL TABLE std5_80.plan_ext(LIKE std5_80.plan)
			LOCATION ('pxf://gp.plan?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER=intern&PASS=intern'
			) ON ALL
			FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import')
			ENCODING 'UTF-8'
TEMP TABLE IS: DROP TABLE IF EXISTS std5_80.plan_tmp;
			CREATE TABLE std5_80.plan_tmp (LIKE std5_80.plan) with (appendonly=true, orientation=column, compresstype=zstd, compresslevel=1) DISTRIBUTED BY (region);
table "plan_tmp" does not exist, skipping
INSERTED ROWS: 0
EXCHANGE PARTITION SCRIPT: ALTER TABLE std5_80.plan EXCHANGE PARTITION FOR (DATE '1997-02-01') WITH TABLE std5_80.plan_tmp WITH VALIDATION

 **/


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
