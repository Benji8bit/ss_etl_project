-- New script in adb.
-- Date: Feb 18, 2024
-- Time: 4:00:11 PM
-------------------------------

/*
Для итогового проекта вам потребуются следующие таблицы:

Название	Техническое имя	Описание
*/
-----------------------------------------------------------------------
/* 1
Магазины	stores	Таблица текстов для магазинов. 
Эта таблица должна быть загружена из файла через gpfdist с локальной машины.
*/
---------stores---------CREATE TABLE------------------------------------

CREATE TABLE std5_80.stores (
	plant VARCHAR(20) NULL,
	txt TEXT NULL
)
DISTRIBUTED REPLICATED;

---------stores---------CREATE EXTERNAL TABLE----------------------------

CREATE EXTERNAL TABLE adb.std5_80.stores_ext (
	plant VARCHAR,
	txt TEXT
)
LOCATION (
	'gpfdist://172.16.128.118:8080/stores.csv'
) ON ALL
FORMAT 'CSV' ( DELIMITER ';' NULL '' ESCAPE '"' QUOTE '"' HEADER )
ENCODING 'UTF8'
SEGMENT REJECT LIMIT 3 ROWS;

---------stores-------CHECKING------------------------------------------

--DROP EXTERNAL TABLE adb.std5_80.stores_ext CASCADE;

--DROP TABLE adb.std5_80.stores CASCADE;

SELECT *
FROM adb.std5_80.stores_ext;

SELECT *
FROM adb.std5_80.stores;

------------------------------------------------------------------------
/* 2
Трафик	traffic	
Информация о входящих в магазин покупателях, 
передаваемая в хранилище данных раз в час с систем учета. 
Эта таблица должна быть загружена из внешней БД PostgreSQL через PXF. 
Параметры подключения:
Хост: 192.168.214.212 
Порт: 5432
БД: postgres
Пользователь: intern
Пароль: intern
*/
--------traffic-----------CREATE TABLE--------------------------------

CREATE TABLE std5_80.traffic (
	plant VARCHAR(4) NULL,
	"date" DATE NULL,
	"time" VARCHAR(20) NULL,
	frame_id VARCHAR(10) NULL,
	quantity INT4 NULL
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED RANDOMLY
PARTITION BY RANGE("date") 
          (
          START ('2021-01-01'::date) 
          END ('2021-12-31'::date) INCLUSIVE 
          EVERY ('1 mon'::interval)
          );

--------traffic-------CREATE EXTERNAL TABLE-------------------------------

CREATE EXTERNAL TABLE adb.std5_80.traffic_ext (
	plant bpchar,
	date bpchar,
	time int4,
	frame_id int8,
	quantity int4
)
LOCATION (
	'pxf://gp.traffic?PROFILE=JDBC&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER=intern&PASS=intern'
) ON ALL
FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' )
ENCODING 'UTF8'
SEGMENT REJECT LIMIT 3 ROWS;

--------traffic-------CHECKING------------------------------------------

--DROP EXTERNAL TABLE adb.std5_80.traffic_ext CASCADE;
--DROP TABLE adb.std5_80.traffic CASCADE;

SELECT *
FROM std5_80.traffic_ext;

SELECT *
FROM std5_80.traffic;

SELECT gp_segment_id, COUNT(*)
FROM std5_80.traffic
GROUP BY 1;

SELECT (gp_toolkit.gp_skew_coefficient('std5_80.traffic'::REGCLASS)).skccoeff;

------------------------------------------------------------------------
/* 3
Чеки	bills_head, bills_item	Данные по чекам. 
Хранятся в двух отдельных таблицах gp.bills_head и gp.bills_item.
Эти таблицы должны быть загружены из внешней БД PostgreSQL через PXF. 
Параметры подключения:
Хост: 192.168.214.212 
Порт: 5432
БД: postgres
Пользователь: intern
Пароль: intern
*/
---------bills_head, bills_item----CREATE TABLES--------------------------------

CREATE TABLE std5_80.bills_head (
	billnum int4 NULL,
	plant varchar(4) NULL,
	calday date NULL
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED RANDOMLY;

CREATE TABLE std5_80.bills_item (
	billnum int8 NULL,
	billitem int8 NULL,
	material int8 NULL,
	qty int8 NULL,
	netval numeric(17, 2) NULL,
	tax numeric(17, 2) NULL,
	rpa_sat numeric(17, 2) NULL,
	calday date NULL
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED RANDOMLY;

--------bills_head, bills_item-----CREATE EXTERNAL TABLES--------------------------

CREATE EXTERNAL TABLE adb.std5_80.bills_head_ext (
	billnum int4,
	plant varchar,
	calday date
)
LOCATION (
	'pxf://gp.bills_head?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER=intern&PASS=intern'
) ON ALL
FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' )
ENCODING 'UTF8'
SEGMENT REJECT LIMIT 10 ROWS;

CREATE EXTERNAL TABLE adb.std5_80.bills_item_ext (
	billnum int8,
	billitem int8,
	material int8,
	qty int8,
	netval numeric,
	tax numeric,
	rpa_sat numeric,
	calday date
)
LOCATION (
	'pxf://gp.bills_item?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER=intern&PASS=intern'
) ON ALL
FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' )
ENCODING 'UTF8'
SEGMENT REJECT LIMIT 3 ROWS;

------bills_head, bills_item----CHECKING------------------------------------------

SELECT *
FROM std5_80.bills_head; -- 3 213 rows

SELECT *
FROM std5_80.bills_head_ext;

SELECT COUNT(*) 
FROM std5_80.bills_head_ext; -- 9 967 126 rows

SELECT *
FROM std5_80.bills_item; -- 20 458 352 rows

SELECT *
FROM std5_80.bills_item_ext; -- 10 566 rows

SELECT COUNT(*) 
FROM std5_80.bills_item_ext; -- 20 458 352 rows

SELECT gp_segment_id, COUNT(*)
FROM std5_80.bills_head
GROUP BY 1;

SELECT (gp_toolkit.gp_skew_coefficient('std5_80.bills_head'::REGCLASS)).skccoeff;

SELECT gp_segment_id, COUNT(*)
FROM std5_80.bills_item
GROUP BY 1;

SELECT (gp_toolkit.gp_skew_coefficient('std5_80.bills_item'::REGCLASS)).skccoeff;

------------------------------------------------------------------------
/* 4
Купоны	coupons	
Данные по скидочным купонам. Регистрация купона продавцом происходит в момент предъявления купона покупателем. 
Каждый купон пробивается в конкретный чек без привязки к позиции. Один купон позволяет применить скидку ровно на 1 ед товара. К
аждый купон относится на конкретную акцию, в рамках которой он был выпущен. 
Проверка на невозможность пробить больше купонов, чем есть продукции в чеке, была выполнена в момент пробития чека.
Эта таблица должна быть загружена из файла через gpfdist с локальной машины.
*/
----------coupons---------CREATE TABLE------------------------------------

CREATE TABLE std5_80.coupons (
	plant varchar(20) NULL,
	"date" date NULL,
	coupon_number varchar(50) NULL,
	promo_id varchar(150) NULL,
	material INT8 NULL,
	billnum INT8 NULL,
	product_cheque VARCHAR(100) NULL,
	price_in_cheque INT4 NULL,
	promo_type INT4 NULL,
	discount NUMERIC(10, 2) NULL
)
DISTRIBUTED RANDOMLY
PARTITION BY RANGE("date") 
          (
          START ('2021-01-01'::date) 
          END ('2021-12-31'::date) INCLUSIVE 
          EVERY ('1 mon'::interval)
          );

ALTER TABLE adb.std5_80.coupons
SET DISTRIBUTED BY (coupon_number);


----------coupons--------CREATE EXTERNAL TABLE----------------------------

CREATE EXTERNAL TABLE adb.std5_80.coupons_ext (
	plant varchar,
	"date" date,
	coupon_number varchar,
	promo_id varchar,
	material INT8,
	billnum INT8,
	product_cheque varchar,
	price_in_cheque int4,
	promo_type int4,
	discount numeric
)
LOCATION (
	'gpfdist://172.16.128.118:8080/coupons.csv'
) ON ALL
FORMAT 'CSV' ( delimiter ';' null '' escape '"' quote '"' header )
ENCODING 'UTF8'
SEGMENT REJECT LIMIT 5 ROWS;

DROP EXTERNAL TABLE adb.std5_80.coupons_ext CASCADE;

DROP TABLE adb.std5_80.coupons CASCADE;

---------coupons-------CHECKING------------------------------------------

SELECT *
FROM adb.std5_80.coupons_ext;

SELECT COUNT(*)
FROM adb.std5_80.coupons_ext;

SELECT *
FROM adb.std5_80.coupons_ext;

SELECT gp_segment_id, COUNT(*)
FROM std5_80.coupons
GROUP BY 1;

SELECT (gp_toolkit.gp_skew_coefficient('std5_80.coupons'::REGCLASS)).skccoeff;


------------------------------------------------------------------------
/* 5
Акции	promos	
Перечень актуальных на момент составления отчета промоакций в компании.
Эта таблица должна быть загружена из файла через gpfdist с локальной машины.
*/
--------promos--------CREATE TABLE--------------------------------------

CREATE TABLE std5_80.promos (
	promo_id bpchar(32) NULL,
	txt varchar NULL,
	"type" int4 NULL,
	material int8 NULL,
	discount int4 NULL
)
DISTRIBUTED REPLICATED;

--------promos-------CREATE EXTERNAL TABLE------------------------------

CREATE EXTERNAL TABLE adb.std5_80.promos_ext (
	promo_id varchar,
	promo_name varchar,
	promo_type varchar,
	product_id varchar,
	discount int4
)
LOCATION (
	'gpfdist://172.16.128.118:8080/promos.csv'
) ON ALL
FORMAT 'CSV' ( delimiter ';' null '' escape '"' quote '"' header )
ENCODING 'UTF8'
SEGMENT REJECT LIMIT 5 ROWS;

--------promos--------CHECKING------------------------------------------

DROP EXTERNAL TABLE adb.std5_80.promos_ext CASCADE;

DROP TABLE adb.std5_80.promos CASCADE;

SELECT *
FROM adb.std5_80.promos;

SELECT *
FROM adb.std5_80.promos_ext;

------------------------------------------------------------------------
/* 6
Тип акции	promo_types	
Перечень актуальных на момент составления отчета типов акций в компании. 
Эта таблица должна быть загружена из файла через gpfdist с локальной машины.
*/
----------------------CREATE TABLE--------------------------------------

CREATE TABLE std5_80.promo_types (
	promo_type varchar(10) NULL,
	txt text NULL
)
DISTRIBUTED REPLICATED;

---------promo_types----CREATE EXTERNAL TABLE--------------------------

CREATE EXTERNAL TABLE adb.std5_80.promo_types_ext (
	promo_type varchar,
	txt text
)
LOCATION (
	'gpfdist://172.16.128.118:8080/promo_types.csv'
) ON ALL
FORMAT 'CSV' ( delimiter ';' null '' escape '"' quote '"' header )
ENCODING 'UTF8'
SEGMENT REJECT LIMIT 5 ROWS;

--------promo_types-----CHECKING----------------------------------------

DROP EXTERNAL TABLE adb.std5_80.promo_types_ext CASCADE;

SELECT *
FROM adb.std5_80.promo_types;

SELECT *
FROM adb.std5_80.promo_types_ext;

------------------------------------------------------------------------
/*
Данные для всех таблиц, кроме таблицы Чеки, в файле во вложении. 
При создании объектов БД следует использовать отдельную схему std##, где ## Ваш ID. 
В названия объектов Apache Superset нужно вставлять префикс STD##. 
*/


