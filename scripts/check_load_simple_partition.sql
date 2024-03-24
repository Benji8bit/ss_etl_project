-- New script in adb.
-- Date: Jan 29, 2024
-- Time: 12:37:54 AM
-------------------------------

SELECT std5_80.f_load_simple_partition ('std5_80.plan', 'date',
										'2021-02-01', '2021-03-01',
										'gp.plan', 'intern', 'intern');
										
SELECT *
FROM std5_80.plan;

DROP TABLE std5_80.plan CASCADE;

SELECT std5_80.f_load_simple_partition ('std5_80.sales', 'date',
										'2021-02-01', '2021-03-01',
										'gp.sales', 'intern', 'intern');
										
SELECT *
FROM std5_80.sales;