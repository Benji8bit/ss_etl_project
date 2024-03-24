-- New script in adb.
-- Date: Feb 3, 2024
-- Time: 4:06:25 AM
-------------------------------

/*
	Функция для создания партиций за определённый период времени. Здесь создаём партиции за месяц, 
	которые в дальнейшем будем использовать в функции для реализации алгоритма delta partition.
	Данная функция будет внутренней для функции f_load_data_mart, которая формирует витрину данных 
	для слоя CDM. Использовать её будем в цикле перебора даты.  	
  */

CREATE OR REPLACE FUNCTION std5_80.f_create_date_partitions(p_table_name TEXT, p_partition_value TIMESTAMP)
	RETURNS VOID
	LANGUAGE plpgsql
	VOLATILE
 AS $$
 DECLARE 
 	v_cnt_partitions INT;
 	v_table_name TEXT;
 	v_partition_end_sql TEXT;
 	v_partition_end TIMESTAMP;
 	v_interval INTERVAL;
 	v_ts_format TEXT := 'YYYY-MM-DD HH24:MI:SS';
 BEGIN
 	v_table_name = std5_80.f_unify_name(p_table_name);
 	-- Проверка наличия партиций у таблицы
 	SELECT COUNT(*) INTO v_cnt_partitions FROM pg_partitions p WHERE p.schemaname||p.tablename = LOWER(v_table_name);
 	
 	IF v_cnt_partitions > 1 THEN
 		LOOP
	 		-- Получение параметров последней партиции
	 		SELECT partitionrangeend INTO v_partition_end_sql
	 			FROM (
	 				SELECT p.*, RANK() OVER (ORDER BY partitionrank DESC) rnk FROM pg_partitions p
	 				WHERE p.partitionrank IS NOT NULL AND p.schemaname||'.'||p.tablename = LOWER(v_table_name)	 				
	 			) q
 			WHERE rnk = 1;
 			-- Конечная дата последней партиции
 			EXECUTE 'SELECT'||v_partition_end_sql INTO v_partition_end;
 			-- Если партиция уже есть для входного значения, тогда EXIT из функции
 			EXIT WHEN v_partition_end > p_partition_value;
 			v_interval := '1 month'::INTERVAL;
 			-- Вырез новой партиции из дефолтной партиции, если её ещё не существует
 			EXECUTE 'ALTER TABLE '||v_table_name||' SPLIT DEFAULT PARTITION
					 START ('||v_partition_end_sql||') END ('''||TO_CHAR(v_partition_end+v_interval, v_ts_format)||'''::TIMESTAMP)';
 		END LOOP;
 	END IF;
END;
$$
EXECUTE ON ANY;