-- New script in adb.
-- Date: Feb 20, 2024
-- Time: 11:43:50 PM
---------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION std5_80.f_project_load_full(p_table TEXT, p_file_name TEXT)
	RETURNS INT4
	LANGUAGE plpgsql
	VOLATILE
AS $$
DECLARE
	v_ext_table_name TEXT;
	v_sql TEXT;
	v_gpfdist TEXT;
	v_result INT4;

BEGIN
	v_ext_table_name = p_table||'_ext';
	EXECUTE 'TRUNCATE TABLE '||p_table;
	EXECUTE 'DROP EXTERNAL TABLE IF EXISTS '||v_ext_table_name;
	v_gpfdist = 'gpfdist://172.16.128.118:8080/'||p_file_name||'.csv';
	v_sql = 'CREATE EXTERNAL TABLE '||v_ext_table_name||' (LIKE '||p_table||')
			LOCATION ('''||v_gpfdist||''') ON ALL
			FORMAT ''CSV'' (DELIMITER '';'' NULL '''' ESCAPE ''"'' QUOTE ''"'' HEADER)
			ENCODING ''UTF8''
			SEGMENT REJECT LIMIT 10 ROWS;';
	RAISE NOTICE 'EXTERNAL TABLE IS: %', v_sql;
	EXECUTE v_sql;
	EXECUTE 'INSERT INTO '||p_table||' SELECT * FROM '||v_ext_table_name;
	EXECUTE 'SELECT COUNT(*) FROM '||p_table INTO v_result;
	RETURN v_result;
END;

$$
EXECUTE ON ANY;