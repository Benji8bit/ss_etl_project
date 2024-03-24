-- New script in adb.
-- Date: Feb 4, 2024
-- Time: 2:24:58 AM
-------------------------------

CREATE OR REPLACE FUNCTION std5_80.f_unify_name(p_name TEXT)
	RETURNS TEXT
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
    /*Kuzin Maxim
    * for Sapiens Solutions 
    * 2024*/
/*Function unifies table name, column name and other names*/
DECLARE
BEGIN
  RETURN lower(trim(translate(p_name, ';/''','')));
END;


$$
EXECUTE ON ANY;