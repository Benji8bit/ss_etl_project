-- New script in adb.
-- Date: Jan 29, 2024
-- Time: 5:30:51 AM
-------------------------------

CREATE OR REPLACE FUNCTION std5_80.f_load_write_log(p_log_type TEXT,
													p_log_message TEXT,
													p_location TEXT)
	RETURNS VOID
	LANGUAGE plpgsql
	VOLATILE 
AS $$

DECLARE 

	v_log_type TEXT;
	v_log_message TEXT;
	v_sql TEXT;
	v_location TEXT;
	v_res TEXT;
	
BEGIN
	
	-- CHeck message type
	v_log_type = UPPER(p_log_type);
	v_location = LOWER(p_location);
	IF v_log_type NOT IN ('ERROR', 'INFO') THEN
		RAISE EXCEPTION 'Illegal log type! Use one of: ERROR, INFO';
	END IF;

	RAISE NOTICE '%: %: <%> Location[%]', CLOCK_TIMESTAMP(), v_log_type, p_log_message, v_location;

	v_log_message := REPLACE(p_log_message, '''', '''''');

	v_sql := 'INSERT INTO std5_80.logs(log_id, log_type, log_msg, log_location, is_error, log_timestamp, log_user)
				VALUES ( ' || nextval('std5_80.log_id_seq')|| ' ,
					   ''' || v_log_type || ''',
						 ' || COALESCE('''' || v_log_message || '''', '''empty''')|| ',
						 ' || COALESCE('''' || v_location || '''', 'null')|| ',
						 ' || CASE WHEN v_log_type = 'ERROR' THEN TRUE ELSE FALSE END || ',
						 current_timestamp, current_user);';
						
	RAISE NOTICE 'INSERT SQL IS: %', v_sql;
	-- Удалённое выполнение запроса со вставкой логов
	v_res := dblink('adb_server', v_sql);

	END;

$$
EXECUTE ON ANY;