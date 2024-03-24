-- New script in adb.
-- Date: Feb 21, 2024
-- Time: 1:02:22 AM
-------------------------------

CREATE OR REPLACE FUNCTION std5_80.f_project_load_mart(p_start_date TIMESTAMP, p_end_date TIMESTAMP)
	RETURNS INT4
	LANGUAGE plpgsql
	VOLATILE
AS $$

DECLARE
	v_sql TEXT;
	v_params TEXT;
	v_start_date DATE;
	v_end_date DATE;
	v_iter_date DATE;
	v_load_interval INTERVAL;
	v_cnt_prt INT8;
	v_cnt INT8;

BEGIN	
	SELECT COALESCE('WITH ('||ARRAY_TO_STRING(reloptions, ', ')||')','')
	FROM pg_class INTO v_params
	WHERE oid = 'std5_80.stores_mart'::REGCLASS;
	
	v_load_interval = '3 month'::INTERVAL;
	v_start_date = DATE_TRUNC('month', p_start_date);
	v_end_date = DATE_TRUNC('month', p_end_date);
	v_iter_date = v_start_date;
	v_cnt = 0;

	WHILE v_iter_date <= v_end_date LOOP	
		DROP TABLE IF EXISTS std5_80.stores_mart_tmp;
		v_sql = 'CREATE TABLE std5_80.stores_mart_tmp (LIKE std5_80.stores_mart) '||v_params||' DISTRIBUTED RANDOMLY;';
	
		RAISE NOTICE 'TEMP TABLE IS: %', v_sql;
		
		EXECUTE v_sql;		
		
		INSERT INTO std5_80.stores_mart_tmp
		WITH cte_bills AS
		(SELECT 
			plant,
			bh.calday,
			SUM(rpa_sat) revenue,
    		SUM(bi.qty) sold_qty,
		    COUNT(DISTINCT bh.billnum) bills_qty
		FROM std5_80.bills_head bh
		JOIN std5_80.bills_item bi ON bh.billnum = bi.billnum
		WHERE bi.calday >= v_iter_date AND bi.calday < v_iter_date + v_load_interval
		GROUP BY plant, bh.calday),
		cte_promos AS
		(SELECT
			plant,
			calday,
			SUM(CASE 
		        WHEN p."type" = 1 THEN c.discount
		        WHEN p."type" = 2 THEN (c.discount * 0.01) * b.price
		        ELSE 0
		    END) coupon_disc,
		    COUNT(*) promo_qty
		FROM (SELECT DISTINCT billnum, material, calday, rpa_sat/qty price FROM std5_80.bills_item) b
			JOIN std5_80.coupons c USING(billnum, material)
			JOIN std5_80.promos p ON c.promo_id = p.promo_id 
		WHERE c.date >= v_iter_date AND c.date < v_iter_date + v_load_interval
		GROUP BY plant, calday),
		cte_traffic AS
		(SELECT 
			plant, 
			date AS calday,
			SUM(quantity) traffic
		FROM std5_80.traffic
		WHERE date >= v_iter_date AND date < v_iter_date + v_load_interval
		GROUP BY plant, calday)
		SELECT 
			plant,
			calday,
			revenue,
			coupon_disc,
			sold_qty,
			bills_qty,
			traffic,
			promo_qty
		FROM cte_traffic
			LEFT JOIN cte_bills USING(plant, calday)
			LEFT JOIN cte_promos USING(plant, calday);
		
		GET DIAGNOSTICS v_cnt_prt = ROW_COUNT;
		RAISE NOTICE 'INSERTED ROWS: %', v_cnt_prt;
		
		v_sql = 'ALTER TABLE std5_80.stores_mart EXCHANGE PARTITION FOR (date '''||v_iter_date||''') WITH TABLE std5_80.stores_mart_tmp WITH VALIDATION';
		
		RAISE NOTICE 'EXCHANGE PARTITION SCRIPT: %', v_sql;
		
		EXECUTE v_sql;
		
		v_cnt = v_cnt + v_cnt_prt;
		v_iter_date = v_iter_date + v_load_interval;
	END LOOP;
	
	RETURN v_cnt;
END;

$$
EXECUTE ON ANY;
