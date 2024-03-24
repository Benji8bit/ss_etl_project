-- New script in adb.
-- Date: Jan 29, 2024
-- Time: 5:33:59 AM
-------------------------------

CREATE OR REPLACE FUNCTION std5_80.f_load_mart(p_month VARCHAR)
	RETURNS INT4
	LANGUAGE plpgsql
	VOLATILE
AS $$

DECLARE 
	v_table_name TEXT;
	v_sql TEXT;
	v_return INT;

BEGIN
	
	PERFORM std5_80.f_load_write_log(p_log_type := 'INFO',
									 p_log_message := 'Start f_load_mart',
									 p_location := 'Sales mart calculation');
									
	DROP TABLE IF EXISTS std5_80.plan_fact_202102;
		CREATE TABLE std5_80.plan_fact_202102
			WITH (
				appendonly=TRUE,
				orientation=COLUMN,
				compresstype=zstd,
				compresslevel=1)
			AS 
				SELECT region, material, distr_chan, SUM(quantity) qnt, COUNT(DISTINCT check_nm) chk_cnt
				FROM std5_80.sales s 
				WHERE date BETWEEN date_trunc('month', to_date(p_month, 'YYYYMM')) - INTERVAL '3 month'
					AND date_trunc('month', to_date(p_month, 'YYYYMM'))
				GROUP BY 1, 2, 3
			DISTRIBUTED BY (material);
		
	SELECT count(*) INTO v_return FROM std5_80.plan_fact_202102;

	PERFORM std5_80.f_load_write_log(p_log_type := 'INFO',
									 p_log_message := v_return || ' rows inserted',
									 p_location := 'Sales mart calculation');
									
	PERFORM std5_80.f_load_write_log(p_log_type := 'INFO',
									 p_log_message := 'End f_load_mart',
									 p_location := 'Sales mart calculation');
									
	RETURN v_return;
	
END;
$$
EXECUTE ON ANY;
