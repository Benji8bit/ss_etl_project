-- New script in adb.
-- Date: Feb 4, 2024
-- Time: 2:42:32 AM
-------------------------------

CREATE OR REPLACE FUNCTION ${target_schema}.f_get_table_attributes(p_table_name text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
		
DECLARE
  v_location text := '${target_schema}.f_get_table_attributes';
  v_params text;
BEGIN
  
	select coalesce('with (' || array_to_string(reloptions, ', ') || ')','')
	from pg_class  
	into v_params
	where oid = ${target_schema}.f_unify_name(p_name := p_table_name)::regclass;
	return v_params;
END;

$$
EXECUTE ON ANY;