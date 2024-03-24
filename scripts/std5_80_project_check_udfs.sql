-- New script in adb.
-- Date: Feb 21, 2024
-- Time: 2:58:47 AM
-----------------------------------stores--------------------------------------

SELECT std5_80.f_project_load_full('std5_80.stores', 'stores');

SELECT *
FROM std5_80.stores;
-----------------------------------coupons-------------------------------------

--SELECT std5_80.f_project_load_full('std5_80.coupons', 'coupons');

SELECT std5_80.f_project_load_delta_partition('coupons', 
									  		  'date', 
									  		  '2021-01-01',
									  		  '2021-03-01');

SELECT *
FROM std5_80.coupons_ext;

SELECT *
FROM std5_80.coupons;

DROP TABLE std5_80.coupons CASCADE;

CREATE TABLE std5_80.coupons (
	plant bpchar(4) NULL,
	"date" date NULL,
	coupon bpchar(7) NULL,
	promo bpchar(32) NULL,
	material int8 NULL,
	billnum int8 NULL
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED BY (billnum);

-------------------------------promos------------------------------------

SELECT std5_80.f_project_load_full('std5_80.promos', 'promos');

SELECT *
FROM std5_80.promos;
-------------------------------promo_types--------------------------------

SELECT std5_80.f_project_load_full('std5_80.promo_types', 'promo_types');

SELECT *
FROM std5_80.promo_types;

------------------------------traffic------------------------------------

DROP TABLE std5_80.traffic CASCADE;

SELECT std5_80.f_project_load_traffic('traffic', 
							  		  'date', 
							  		  '2021-01-01',
							  		  '2021-03-01');
							  		 
SELECT std5_80.f_project_load_delta_partition('traffic', 
									  		  'date', 
									  		  '2021-01-01',
									  		  '2021-03-01');
									
SELECT *
FROM std5_80.traffic_ext;

SELECT *
FROM std5_80.traffic;

TRUNCATE std5_80.traffic;

-----------------------------bills_head--------------------------------

DROP TABLE std5_80.bills_head CASCADE;

CREATE TABLE std5_80.bills_head (
	billnum INT8 NULL,
	plant VARCHAR(10) NULL,
	calday DATE NULL
)
DISTRIBUTED RANDOMLY
PARTITION BY RANGE (calday)
(
	START (DATE '2021-01-01') INCLUSIVE 
	END (DATE '2021-12-31') INCLUSIVE
	EVERY (INTERVAL '1 MONTH')
);

SELECT std5_80.f_project_load_delta_partition('bills_head', 
									  		  'calday', 
									  		  '2021-01-01',
									  		  '2021-03-01');
									
SELECT *
FROM std5_80.bills_head_ext;

SELECT *
FROM std5_80.bills_head;

-----------------------------bills_item--------------------------------

DROP TABLE std5_80.bills_item CASCADE;

CREATE TABLE std5_80.bills_item (
	billnum int8 NULL,
	billitem int8 NULL,
	material int8 NULL,
	qty int8 NULL,
	netval numeric NULL,
	tax numeric NULL,
	rpa_sat numeric NULL,
	calday date NULL
)
DISTRIBUTED RANDOMLY
PARTITION BY RANGE (calday)
(
	START (DATE '2021-01-01') INCLUSIVE 
	END (DATE '2021-12-31') INCLUSIVE
	EVERY (INTERVAL '1 MONTH')
);

SELECT std5_80.f_project_load_delta_partition('bills_item', 
									  		  'calday', 
									  		  '2021-01-01',
									  		  '2021-03-01');
									
SELECT *
FROM std5_80.bills_item_ext;

SELECT *
FROM std5_80.bills_item;

------------------------stores_mart----------------------

CREATE TABLE std5_80.stores_mart (
	plant BPCHAR(4) NULL,
	calday DATE NULL,
	revenue NUMERIC(9, 2) NULL,
	coupon_disc NUMERIC(9, 2) NULL,
	sold_qty INT4 NULL,
	bills_qty INT4 NULL,
	traffic INT4 NULL,
	promo_qty INT4 NULL
)
WITH (
	appendonly = TRUE,
	orientation = column,
	compresstype = zstd,
	compresslevel = 1
)
DISTRIBUTED RANDOMLY
PARTITION BY RANGE (calday)
(
	START (DATE '2021-01-01') INCLUSIVE 
	END (DATE '2024-12-31') INCLUSIVE
	EVERY (INTERVAL '3 MONTH')
);

SELECT *
FROM std5_80.stores_mart;

ANALYZE std5_80.stores_mart;

DROP TABLE std5_80.stores_mart_tmp CASCADE;

DROP TABLE std5_80.stores_mart CASCADE;

SELECT std5_80.f_project_load_mart('2021-01-01',
								   '2021-03-01'); --I can DO it.