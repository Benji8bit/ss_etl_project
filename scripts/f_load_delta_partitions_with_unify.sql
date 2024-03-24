-- New script in adb.
-- Date: Feb 3, 2024
-- Time: 4:41:24 AM
-------------------------------

/*
1. Создайте 2 пользовательские функции в схеме std <номер студента> для загрузки данных в созданные на 2-ом уроке таблицы: 
Загрузка данных в целевые таблицы должна производиться из внешних EXTERNAL таблиц.
Первая функция для загрузки справочников, вторая - для загрузки таблиц фактов.
Для таблиц справочников необходимо реализовать FULL загрузку (полная очистка целевой таблицы и полная вставка всех записей).
Для таблиц фактов можно реализовать загрузку следующими способами:
DELTA_PARTITION - полная подмена партиций.
DELTA_UPSERT - предварительное удаление по ключу и последующая вставка записей из временной таблицы в целевую.

Здесь мы реализуем алгоритм DELTA PARTITION
с дополнительными пользовательскими функциями.*/

-------------------------------

--DROP FUNCTION std5_80.f_load_delta_partitions(TEXT, TEXT, TEXT, TEXT, TIMESTAMP, TIMESTAMP);


CREATE OR REPLACE FUNCTION std5_80.f_load_delta_partitions(p_table_from_name TEXT, p_table_to_name TEXT,
														   p_partition_key TEXT, p_schema_name TEXT,
														   p_start_date TIMESTAMP, p_end_date TIMESTAMP)
	RETURNS INT8
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE 
AS $$

DECLARE 
	v_table_from_name TEXT;
	v_table_to_name TEXT;
	v_start_date DATE;
	v_end_date DATE;
	v_load_interval INTERVAL;
	v_iterDate TIMESTAMP;
	v_where TEXT;
	v_prt_table TEXT;
	v_cnt_prt INT8;
	v_cnt INT8;
	p_name TEXT;
	v_schema_name TEXT;
	p_user_id TEXT = 'intern';
	p_pass TEXT = 'intern';
	

BEGIN
	
	v_table_from_name = std5_80.f_unify_name(p_table_from_name);
	v_table_to_name = std5_80.f_unify_name(p_table_to_name);

	PERFORM std5_80.f_create_date_partitions(v_table_to_name, p_end_date);
	v_load_interval = '1 month'::INTERVAL;
	v_start_date := DATE_TRUNC('month', p_start_date);
	v_end_date := DATE_TRUNC('month', p_end_date) + v_load_interval;
	LOOP
		v_iterDate = v_start_date + v_load_interval;
		EXIT WHEN (v_iterDate > v_end_date);
		--v_prt_table = std5_80.f_create_tmp_table(p_table_name := v_table_to_name,
		--											 p_schema_name := v_schema_name,
		--											 p_prefix_name := 'prt_',
		--											 p_suffix_name := '_'||TO_CHAR(v_start_date, 'YYYYMMDD'));
		v_where = p_partition_key || '>='''||v_start_date|| '''::TIMESTAMP AND '||p_partition_key||'<'''||v_iterDate||'''TIMESTAMP';
		/*v_cnt_prt = std5_80.f_insert_table(p_table_name := v_table_from_name, 
											   p_table_to := v_prt_table, p_where := v_where);*/
		v_cnt = v_cnt + v_cnt_prt;
		/*PERFORM std5_80.f_switch_partition(p_table_name := v_table_to_name, 
											   p_partition_value := v_start_date,
											   p_switch_table_name := v_prt_table);*/
		--EXECUTE 'DROP TABLE '||v_prt_table;
											  
		PERFORM std5_80.f_load_simple_partition(p_table_to_name, p_partition_key,
												p_start_date, p_end_date,
												p_table_from_name, p_user_id, p_pass);									  
											  
		v_start_date := v_iterDate;		
	END LOOP;
	
  RETURN v_cnt;	
END;
$$
EXECUTE ON ANY;