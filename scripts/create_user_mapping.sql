-- New script in adb.
-- Date: Jan 29, 2024
-- Time: 6:05:23 AM
-------------------------------

CREATE USER MAPPING FOR std5_80 SERVER adb_server 
OPTIONS (user 'std5_80', password '9HjeACizqp8T');

SELECT std5_80.f_load_mart('202104');

SELECT *
FROM std5_80.plan_fact_202102;

SELECT *
FROM std5_80.logs;