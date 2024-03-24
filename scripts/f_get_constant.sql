-- New script in adb.
-- Date: Feb 4, 2024
-- Time: 3:13:31 AM
-------------------------------

DROP FUNCTION std5_80.f_get_constant(text);

CREATE OR REPLACE FUNCTION ${target_schema}.f_get_constant(p_constant_name text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
    /*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Get constant value from table load_constants*/
declare
    v_location text := '${target_schema}.f_get_constant';
    v_res text;
    v_sql text;
BEGIN
     v_sql = 'select constant_value from ${target_schema}.load_constants where constant_name = '''||${target_schema}.f_unify_name(p_name := p_constant_name)||'''';
     execute v_sql into v_res;
     return v_res;
 exception when others then 
  raise notice 'ERROR get constant with name %, ERROR: %',p_constant_name,SQLERRM;
  PERFORM ${target_schema}.f_write_log(
     p_log_type    := 'ERROR', 
     p_log_message := 'Get constant with name '|| p_constant_name||' finished with error, ERROR: '||SQLERRM, 
     p_location    := v_location);
   return null;
END;


$$
EXECUTE ON ANY;