-- New script in adb.
-- Date: Feb 4, 2024
-- Time: 2:45:47 AM
-------------------------------

CREATE OR REPLACE FUNCTION ${target_schema}.f_get_distribution_key(p_table_name text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
    /*Ismailov Dmitry
    * Sapiens Solutions 
    * 2023*/
/*Function return distribution key*/
DECLARE
  v_location text := '${target_schema}.f_get_distribution_key';
  v_table_name text;
  v_dist_key text;
  v_table_oid int4;
BEGIN

v_table_name = ${target_schema}.f_unify_name(p_table_name);  
perform ${target_schema}.f_write_log(
     p_log_type := 'SERVICE', 
     p_log_message := 'START get distribution for table '||v_table_name, 
     p_location    := v_location); --log function call
     
select c.oid
 into v_table_oid
  from pg_class as c inner join pg_namespace as n on c.relnamespace = n.oid
 where n.nspname||'.'||c.relname = v_table_name
 limit 1;
 if v_table_oid = 0 or v_table_oid is null then
   v_dist_key = 'DISTRIBUTED RANDOMLY';
 else
   v_dist_key = pg_get_table_distributedby(v_table_oid);
 end if;

perform ${target_schema}.f_write_log(
   p_log_type := 'SERVICE', 
   p_log_message := 'END get distribution for table '||v_table_name||' ,distribution rule: '||v_dist_key, 
   p_location    := v_location); --log function call
 return v_dist_key;
END;

$$
EXECUTE ON ANY;