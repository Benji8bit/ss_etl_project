-- New script in adb.
-- Date: Feb 21, 2024
-- Time: 4:59:09 AM
-------------------------------

CREATE OR REPLACE
FUNCTION std5_80.func_load_traffic(
	p_table TEXT,
	p_partition_key TEXT,
	p_start_date timestamp,
	p_pxf_table TEXT,
	p_user_id TEXT,
	p_pass TEXT
)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
AS $$	
	
DECLARE
v_ext_table TEXT;

v_temp_table TEXT;

v_table_oid int4;

v_sql TEXT;

v_pxf TEXT;

v_result int;

v_dist_key TEXT;

v_params TEXT;

v_where TEXT;

v_load_interval INTERVAL;

v_start_date date;

v_start_date_tg TEXT;

v_start_date_src TEXT;

v_end_date date;

lv_res int4;

v_cnt int8;

lt_tab record;

v_check TEXT;

BEGIN 

v_ext_table = p_table || '_ext';

v_temp_table = p_table || '_tmp';

SELECT
	c.oid
INTO
	v_table_oid
FROM
	pg_class AS c
INNER JOIN pg_namespace AS n ON
	c.relnamespace = n.oid
WHERE
	n.nspname || '.' || c.relname = p_table
LIMIT 1;

IF v_table_oid = 0
OR v_table_oid IS NULL THEN 
v_dist_key = 'DISTRIBUTED RANDOMLY';
ELSE
v_dist_key = pg_get_table_distributedby(v_table_oid);
END IF;

SELECT
	COALESCE(' with (' || array_to_string(reloptions, ', ')|| ')', '')
FROM
	pg_class
WHERE
	oid = p_table::regclass
INTO
	v_params;

v_load_interval = '1 month'::INTERVAL;

EXECUTE 'DROP EXTERNAL TABLE IF EXISTS ' || v_ext_table;

v_pxf = 'pxf://' || p_pxf_table || '?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER=' || p_user_id || '&PASS=' || p_pass;

RAISE NOTICE 'PXF CONNECTION STRING: %',
v_pxf;

v_sql = 'CREATE EXTERNAL TABLE ' || v_ext_table || '(plant bpchar,
												 date bpchar,
												 time bpchar,
												 frame_id bpchar,
												 quantity int4)
			 LOCATION (''' || v_pxf || '''
			 ) ON ALL
			 FORMAT ''CUSTOM'' (FORMATTER=''pxfwritable_import'')
			 ENCODING ''UTF8''';

EXECUTE v_sql;

RAISE NOTICE 'EXTERNAL TABLE IS: %',
v_sql;
-- ласт дата в текущей traffic - целевая NULL
v_sql = 'SELECT max(' || p_partition_key || '::date) FROM ' || p_table;

EXECUTE v_sql
INTO
	v_start_date_tg;
-- ласт дата в traffic_ext == gp.traffic - источник 2021-02-28
v_sql = 'SELECT max(to_date(' || p_partition_key || ', ' || quote_literal('DD.MM.YYYY')|| ')) as ' || p_partition_key || ' FROM ' || v_ext_table;

EXECUTE v_sql
INTO
	v_start_date_src;
-- если пустая traffic,то старт дата из параметра '2021-01-01', енд дата - ласт дата в источнике(traffic_ext == gp.traffic)
-- если что-то есть в traffic то старт дата - это ласт дата в текущей traffic (v_start_date_tg), а енд дата - текущая дата
IF v_start_date_tg = ''
OR v_start_date_tg IS NULL
THEN
-- если пустая
	RAISE NOTICE '->>>>>>>>>>>>>>>>>>>>> IF BLOCK';

v_start_date := DATE_TRUNC('month', CAST(p_start_date AS date));
-- будет 2021-01-01
v_end_date = v_start_date_src;
-- будет 2021-02-28
ELSE 
	v_start_date := CAST(v_start_date_tg AS date)+ INTERVAL '1 day';

v_end_date = current_date;
END IF;

WHILE v_start_date <= v_end_date
-- 2021-01-01 <= 2021-02-28

LOOP
	v_sql := 'DROP TABLE IF EXISTS ' || v_temp_table || ';
	CREATE TABLE ' || v_temp_table || ' (LIKE ' || p_table || ') ' || v_params || ' ' || v_dist_key || ';';

RAISE NOTICE 'TEMP TABLE IS: %',
v_sql;

EXECUTE v_sql;
--                      date                                                 >=       2021-01-01             and                  date                                            <       2021-01-01+ 1 month = 2021-02-01   
v_where = 'to_date(' || p_partition_key || ', ' || quote_literal('DD.MM.YYYY')|| ') >=''' || v_start_date || '''::date AND to_date(' || p_partition_key || ', ' || quote_literal('DD.MM.YYYY')|| ') < ''' || v_start_date || '''::date+interval ' || quote_literal('1 month')|| '';

RAISE NOTICE 'v_where: %',
v_where;

v_sql = 'INSERT INTO ' || v_temp_table || ' SELECT plant,
													 to_date(date, ' || quote_literal('DD.MM.YYYY')|| ') as date,
													 TO_TIMESTAMP(time, ' || quote_literal('HH24MISS')|| ') ::TIME as time,
													 frame_id,
													 quantity
	 FROM ' || v_ext_table || ' WHERE ' || v_where;

RAISE NOTICE 'INSERT IS: %',
v_sql;

EXECUTE v_sql;
-- проверка ,если есть хотя бы что-то в traffic_tmp то в v_check
v_sql = 'SELECT * FROM ' || v_temp_table || ' LIMIT 1';

EXECUTE v_sql
INTO
	v_check;
-- меняем партицию
	IF v_check IS NOT NULL
	THEN
--             traffic         2021-01-01
v_sql = '
		ALTER TABLE ' || p_table || ' 
		EXCHANGE PARTITION FOR (DATE ''' || v_start_date || ''') 
		WITH TABLE ' || v_temp_table || ' 
		WITH VALIDATION;';

RAISE NOTICE 'EXCHANGE PARTITION SCRIPT: %',
v_sql;

EXECUTE v_sql;
ELSE 
		v_start_date = v_end_date;
END IF;

v_sql := 'DROP TABLE IF EXISTS ' || v_temp_table;

EXECUTE v_sql;

v_start_date = v_start_date + v_load_interval;
END LOOP;

v_result = 1;

RETURN v_result;
END;

$$
EXECUTE ON
ANY;
