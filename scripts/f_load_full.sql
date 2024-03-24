-- New script in adb.
-- Date: Jan 28, 2024
-- Time: 1:47:10 AM
-------------------------------

/*
1. Создайте 2 пользовательские функции в схеме std <номер студента> для загрузки данных в созданные на 2-ом уроке таблицы: 
Загрузка данных в целевые таблицы должна производиться из внешних EXTERNAL таблиц.
Первая функция для загрузки справочников, вторая - для загрузки таблиц фактов.
Для таблиц справочников необходимо реализовать FULL загрузку (полная очистка целевой таблицы и полная вставка всех записей).
Для таблиц фактов можно реализовать загрузку следующими способами:
DELTA_PARTITION - полная подмена партиций.
DELTA_UPSERT - предварительное удаление по ключу и последующая вставка записей из временной таблицы в целевую.
 * */

-------------------------------

CREATE  OR REPLACE FUNCTION std5_80.f_load_full(p_table TEXT, p_file_name TEXT)
	RETURNS INT4
	LANGUAGE plpgsql
	VOLATILE 
AS $$

DECLARE 

	v_ext_table_name TEXT;
	v_sql TEXT;
	v_gpfdist TEXT;
	v_result TEXT;
	
BEGIN
	
	v_ext_table_name = p_table||'_ext';

	EXECUTE 'TRUNCATE TABLE '||p_table;

	EXECUTE 'DROP EXTERNAL TABLE IF EXISTS '||v_ext_table_name;

	v_gpfdist = 'GPFDIST://172.16.128.118:8080/'||p_file_name||'.CSV';

	v_sql = 'CREATE EXTERNAL TABLE '||v_ext_table_name||'(LIKE '||p_table||')
			LOCATION ('''||v_gpfdist||'''
			) ON ALL
			FORMAT ''CSV'' ( HEADER DELIMITER '';'' NULL '''' ESCAPE ''"'' QUOTE ''"'' )
			ENCODING ''UTF-8''';
		
	RAISE NOTICE 'EXTERNAL TABLE IS: %', v_sql;

	EXECUTE v_sql;

	EXECUTE 'INSERT INTO '||p_table||' SELECT * FROM '||v_ext_table_name;

	EXECUTE 'SELECT COUNT(1) FROM '||p_table INTO v_result;

	RETURN v_result;
	
END;

$$
EXECUTE ON ANY;
