-- New script in adb.
-- Date: Feb 3, 2024
-- Time: 6:04:29 AM
-------------------------------

-- Здесь мы реализуем алгоритм DELTA PARTITION

/*CREATE OR REPLACE FUNCTION std5_80.f_load_delta_partitions(p_table_from_name TEXT, p_table_to_name TEXT,
														   p_partition_key TEXT, p_schema_name TEXT,
														   p_start_date TIMESTAMP, p_end_date TIMESTAMP)*/

------------------------------------------------------------------------

SELECT std5_80.f_load_delta_partition('std5_80.plan', 'date',
									  '2021-01-01', '2021-12-01',
									  'gp.plan');
									
SELECT *
FROM std5_80.plan;

TRUNCATE std5_80.plan;


SELECT std5_80.f_load_delta_partition('std5_80.sales', 'date',
									  '2021-01-01', '2021-12-31',
									  'gp.sales', 'intern', 'intern');
										
SELECT *
FROM std5_80.sales;	

TRUNCATE std5_80.sales;

