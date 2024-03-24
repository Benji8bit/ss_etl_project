-- New script in adb.
-- DATE: Feb 21, 2024
-- Time: 12:01:00 AM
-------------------------------

CREATE OR REPLACE FUNCTION std5_80.f_project_load_delta_partition(p_table TEXT, 
																  p_partition_key TEXT, 
																  p_start_date TIMESTAMP, 
																  p_end_date TIMESTAMP)
	RETURNS INT4
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
DECLARE
	v_ext_table TEXT;
	v_temp_table TEXT;
	v_sql TEXT;
	v_dist_key TEXT;
	v_params TEXT;
	v_where TEXT;
	v_start_date DATE;
	v_end_date DATE;
	v_iter_date DATE;
	v_load_interval INTerval;
	v_table_oid INT4;
	v_cnt_prt INT8;
	v_cnt INT8;
	v_columns TEXT;

BEGIN
	v_ext_table = p_table||'_ext';
	v_temp_table = p_table||'_tmp';
	SELECT c.oid INTO v_table_oid
	FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
	WHERE n.nspname||'.'||c.relname = p_table
	LIMIT 1;
	
	IF v_table_oid = 0 OR v_table_oid IS NULL THEN 
		v_dist_key = 'DISTRIBUTED RANDOMLY';
	ELSE
		v_dist_key = pg_get_table_distributedby(v_table_oid);
	END IF;
	
	SELECT COALESCE('WITH ('||ARRAY_TO_STRING(reloptions, ', ')||')','')
	FROM pg_class INTO v_params
	WHERE oid = p_table::REGCLASS;
	
	SET datestyle to 'ISO, DMY';
	
	SELECT STRING_AGG(
	CASE 
        WHEN attname = p_partition_key THEN p_partition_key||'::DATE'
        ELSE attname
    END, ', ') INTO v_columns
	FROM pg_catalog.pg_attribute
	WHERE attrelid = p_table::REGCLASS
	AND attnum > 0
	AND NOT attisdropped;

/*
1. В первом запросе сохраняется OID таблицы, имя которой передается в переменной `p_table`. 
	Он ищет OID таблицы в базе данных PostgreSQL по её имени и схеме.
2. Затем происходит проверка найденного OID таблицы.
	Если OID равен 0 или NULL, то переменной `v_dist_key` присваивается значение 'DISTRIBUTED RANDOMLY', 
	в противном случае вызывается функция `pg_get_table_distributedby`, чтобы получить ключ распределения таблицы.
3. В следующем запросе извлекаются параметры таблицы. 
	Они сохраняются в переменной `v_params`. 
	Если у таблицы есть какие-либо параметры, они будут объединены в строку с разделителем ', '.
4. Далее устанавливается формат даты в 'ISO, DMY'. 
	Это определяет формат представления даты для последующих операций.
5. В последнем запросе создается строка из названий столбцов таблицы. 
	Если имя столбца совпадает с ключом разделения `p_partition_key`, то к нему добавляется '::DATE', 
	иначе используется просто имя столбца. Полученная строка сохраняется в переменной `v_columns`.
	Этот код выполняет несколько операций, связанных с извлечением информации о таблице и формированием строк для дальнейшего использования.
*/
	
	v_load_interval = '1 month'::INTERVAL;
	v_start_date = DATE_TRUNC('month', p_start_date);
	v_end_date = DATE_TRUNC('month', p_end_date);
	v_iter_date = v_start_date;
	v_cnt = 0;

	WHILE v_iter_date <= v_end_date LOOP
		
		v_sql = 'DROP TABLE IF EXISTS '||v_temp_table||';
			CREATE TABLE '||v_temp_table||' (LIKE '||p_table||') '||v_params||' '||v_dist_key||';';
	
		RAISE NOTICE 'TEMP TABLE IS: %', v_sql;
		
		EXECUTE v_sql;
		
		v_where = p_partition_key||'::DATE >= '''||v_iter_date||'''::DATE AND '||p_partition_key||'::DATE < '''||v_iter_date + v_load_interval ||'''::DATE';
		v_sql = 'INSERT INTO '||v_temp_table||' SELECT '||v_columns||' FROM '||v_ext_table||' WHERE '||v_where;
		
		EXECUTE v_sql;
		
		GET DIAGNOSTICS v_cnt_prt = ROW_COUNT;
		RAISE NOTICE 'INSERTED ROWS: %', v_cnt_prt;
		
		v_sql = 'ALTER TABLE '||p_table||' EXCHANGE PARTITION FOR (DATE '''||v_iter_date||''') WITH TABLE '||v_temp_table||' WITH VALIDATION';
		
		RAISE NOTICE 'EXCHANGE PARTITION SCRIPT: %', v_sql;
		
		v_sql = 'ANALYZE '||p_table||';';
		
		RAISE NOTICE 'ANALYZING TABLE: %', p_table;
	
		EXECUTE v_sql;
		
		v_cnt = v_cnt + v_cnt_prt;
		v_iter_date = v_iter_date + v_load_interval;
	
	END LOOP;
	
	RETURN v_cnt;
END;

$$
EXECUTE ON ANY;

SELECT *
FROM pg_class;