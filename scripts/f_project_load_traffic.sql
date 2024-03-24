-- New script in adb.
-- Date: Feb 22, 2024
-- Time: 12:16:22 AM
-------------------------------

CREATE OR REPLACE FUNCTION std5_80.f_project_load_traffic(p_table TEXT, 
												  		  p_partition_key TEXT, 
												  		  p_start_date TIMESTAMP, 
												  		  p_end_date TIMESTAMP)
	RETURNS INT4
	LANGUAGE plpgsql
	VOLATILE
AS $$	
	
DECLARE
p_pxf_table TEXT = 'gp.traffic'; 
p_user_id TEXT = 'intern'; 
p_pass TEXT = 'intern';
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
v_table_oid INT4;
v_cnt INT8;

BEGIN	
	v_ext_table = p_table||'_ext';
	v_temp_table = p_table||'_temp';

	SELECT c.oid
		INTO v_table_oid
		FROM pg_class AS c 
		INNER JOIN pg_namespace AS n 
		ON c.relnamespace = n.oid
		WHERE n.nspname||'.'||c.relname = p_table
		LIMIT 1;
	
	IF 
		v_table_oid = 0 OR v_table_oid IS NULL THEN 
		v_dist_key='DISTRIBUTED RANDOMLY';
	ELSE
		v_dist_key = pg_get_table_distributedby(v_table_oid);
	END IF;
	
	SELECT COALESCE ('WITH ('||ARRAY_TO_STRING(reloptions, ',')||')', '')
	FROM pg_class INTO v_params
	WHERE oid=p_table::REGCLASS;

	EXECUTE 'DROP EXTERNAL TABLE IF EXISTS '||v_ext_table;

	v_load_interval = '1 month'::INTERVAL;
	v_start_date = DATE_TRUNC('month',p_start_date);
	v_end_date = DATE_TRUNC('month',p_end_date);
	
	--v_where=p_partition_key||'>='''||v_start_date||'''::date AND '|| p_partition_key ||'< '''||v_end_date||'''::date';

	v_pxf='pxf://'||p_pxf_table||'?PROFILE=JDBC&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER='||p_user_id||'&PASS='||p_pass;
	
RAISE NOTICE 'PXF CONNECTION IS: %', v_pxf;

	v_sql = 'CREATE EXTERNAL TABLE '||v_ext_table||'(plant bpchar,
												 	date bpchar,
												 	time bpchar,
												 	frame_id bpchar,
												 	quantity int4)
			 LOCATION ('''||v_pxf||'''
			 ) ON ALL
			 FORMAT ''CUSTOM'' (FORMATTER = ''pxfwritable_import'')
			 ENCODING ''UTF8''
			 SEGMENT REJECT LIMIT 3 ROWS';

	
	RAISE NOTICE 'EXTERNAL_TABLE IS %: ',v_pxf;
	EXECUTE v_sql;

WHILE v_start_date < p_end_date LOOP
	RAISE NOTICE 'START DATE IS % AND END DATE IS %', v_start_date,v_end_date;

	v_sql='DROP TABLE IF EXISTS '||v_temp_table ||';';
	EXECUTE v_sql;	
	v_sql=' CREATE TABLE '||v_temp_table||' (LIKE '||p_table||') ' ||v_params||' '||v_dist_key||';';
	RAISE NOTICE 'TEMP TABLE IS: %', v_sql;
	EXECUTE v_sql;

	v_end_date = v_start_date + v_load_interval;
	IF v_end_date > p_end_date THEN
				v_end_date = p_end_date;
			END IF;
	v_where = 'to_date('||p_partition_key||'::TEXT, '||QUOTE_LITERAL('DD.MM.YYYY')||') >='''||v_start_date||'''::date AND to_date('||p_partition_key||'::TEXT, '||QUOTE_LITERAL('DD.MM.YYYY')||') < '''||v_end_date||'''::date';

	
	v_sql='INSERT INTO '||v_temp_table|| ' SELECT * FROM '||v_ext_table|| ' WHERE '||v_where;

	v_sql='INSERT INTO '||v_temp_table|| ' SELECT plant,
												  to_date(date, '||quote_literal('DD.MM.YYYY')||') as date,
												  time,
												  frame_id,
												  quantity
	 									   FROM '||v_ext_table||' WHERE '||v_where;

	EXECUTE v_sql;

	GET DIAGNOSTICS v_cnt=ROW_COUNT;
	RAISE NOTICE 'INSERTED ROWS: %', v_cnt;

	v_sql='ALTER TABLE '||p_table|| ' EXCHANGE PARTITION FOR (DATE '''||v_start_date||''') WITH TABLE '||v_temp_table||' WITH VALIDATION';
	RAISE NOTICE 'EXCHANGE PARTITION SCRIPT: %', v_sql;
	EXECUTE v_sql;
	
	v_start_date = v_end_date;
	END LOOP;
	v_sql='DROP TABLE IF EXISTS '||v_temp_table;
	EXECUTe v_sql;
	EXECUTE 'select count(1) from '||p_table into v_result;
	RETURN v_result;
END;

$$
EXECUTE ON ANY;
