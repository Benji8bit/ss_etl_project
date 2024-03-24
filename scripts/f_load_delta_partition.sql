-- New script in adb.
-- Date: Feb 4, 2024
-- Time: 4:34:01 AM
-------------------------------

/*
1. Создайте 2 пользовательские функции в схеме std <номер студента> для загрузки данных в созданные на 2-ом уроке таблицы: 
Загрузка данных в целевые таблицы должна производиться из внешних EXTERNAL таблиц.
Первая функция для загрузки справочников, вторая - для загрузки таблиц фактов.
Для таблиц справочников необходимо реализовать FULL загрузку (полная очистка целевой таблицы и полная вставка всех записей).
Для таблиц фактов можно реализовать загрузку следующими способами:
DELTA_PARTITION - полная подмена партиций.
DELTA_UPSERT - предварительное удаление по ключу и последующая вставка записей из временной таблицы в целевую.

Здесь мы реализуем загрузку данных в целевые таблицы при помощи алгоритма DELTA PARTITION
без использования пользовательских функций
с применением цикла с внутренней функцией SIMPLE PARTITION*/

-------------------------------

CREATE OR REPLACE FUNCTION std5_80.f_load_delta_partition(p_table TEXT, p_partition_key TEXT,
														  p_start_date TIMESTAMP, p_end_date TIMESTAMP,
														  p_pxf_table TEXT)															
	RETURNS INT4
	LANGUAGE plpgsql
	VOLATILE
AS $$

DECLARE

	v_ext_table TEXT;
	v_temp_table TEXT;
	v_sql TEXT;
	v_pxf TEXT;
	v_result INT;
	v_dist_key TEXT;
	v_params TEXT;
	v_where TEXT;
	v_load_interval INTERVAL;
	v_start_date DATE;
	v_end_date DATE;
	v_iterDate TIMESTAMP;
	v_table_oid INT4;
	v_cnt INT8;
	p_user_id TEXT = 'intern';
	p_pass TEXT = 'intern';
	
BEGIN
	
	v_ext_table = p_table||'_ext';
	v_temp_table = p_table||'_tmp';

	SELECT c.oid
	INTO v_table_oid
	FROM pg_class AS c 
	INNER JOIN pg_namespace AS n
	ON c.relnamespace = n.oid
	WHERE n.nspname||'.'||c.relname = p_table
	LIMIT 1;
	
	IF v_table_oid = 0 OR v_table_oid IS NULL THEN
		v_dist_key = 'DISTRIBUTED RANDOMLY';
	ELSE
		v_dist_key = pg_get_table_distributedby(v_table_oid);
	END IF;

	SELECT COALESCE('with (' || ARRAY_TO_STRING(reloptions, ', ') || ')', '')
	FROM pg_catalog.pg_class 
	INTO v_params
	WHERE oid = p_table::REGCLASS;

	EXECUTE 'DROP EXTERNAL TABLE IF EXISTS '||v_ext_table;

	v_load_interval = '1 month'::INTERVAL;
	v_start_date := DATE_TRUNC('month', p_start_date);
	v_end_date := DATE_TRUNC('month', p_end_date) + v_load_interval;

	v_where = p_partition_key ||' >= '''||v_start_date||'''::date AND '||p_partition_key||' < '''||v_end_date||'''::date';

	v_pxf = 'pxf://'||p_pxf_table||'?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER='
			||p_user_id||'&PASS='||p_pass;
		
	RAISE NOTICE 'PXF CONNECTION STRING: %', v_pxf;

	v_sql = 'CREATE EXTERNAL TABLE '||v_ext_table||'(LIKE '||p_table||')
			LOCATION ('''||v_pxf||'''
			) ON ALL
			FORMAT ''CUSTOM'' (FORMATTER=''pxfwritable_import'')
			ENCODING ''UTF-8''
			SEGMENT REJECT LIMIT 10 ROWS';
		
	RAISE NOTICE 'EXTERNAL TABLE IS: %', v_sql;

	EXECUTE v_sql;
	
	LOOP

		v_iterDate = v_start_date + v_load_interval;
	
		EXIT WHEN (v_iterDate > v_end_date);
	
		v_sql := 'DROP TABLE IF EXISTS '|| v_temp_table ||';
				CREATE TABLE '|| v_temp_table ||' (LIKE '||p_table||') ' ||v_params||' '||v_dist_key||';';
			
		RAISE NOTICE 'TEMP TABLE IS: %', v_sql;
	
		EXECUTE v_sql;
	
		v_sql = 'INSERT INTO '|| v_temp_table ||' SELECT * FROM '||v_ext_table||' WHERE '||v_where;
	
		EXECUTE v_sql;
	
		GET DIAGNOSTICS v_cnt = ROW_COUNT;
	
		RAISE NOTICE 'INSERTED ROWS: %', v_cnt;
	
		v_sql = 'ALTER TABLE '||p_table||' EXCHANGE PARTITION FOR (DATE '''||v_start_date||''') WITH TABLE '|| v_temp_table ||' WITH VALIDATION';
	
		RAISE NOTICE 'EXCHANGE PARTITION SCRIPT: %', v_sql;
	
		EXECUTE v_sql;
	
		EXECUTE 'SELECT COUNT(1) FROM '||p_table||' WHERE '||v_where INTO v_result;
	
		v_start_date := v_iterDate;

	END LOOP;
	
	RETURN v_result;
	
END;

$$
EXECUTE ON ANY;