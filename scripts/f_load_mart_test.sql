-- New script in adb.
-- Date: Feb 22, 2024
-- Time: 4:39:31 PM
-------------------------------

DROP FUNCTION std5_80.f_load_mart_test(timestamp, timestamp);

CREATE OR REPLACE FUNCTION std5_80.f_load_mart_test(p_start_date timestamp, p_end_date timestamp)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
AS $$	

declare
	v_sql text;
	v_params text;
	v_start_date date;
	v_end_date date;
	v_iter_date date;
	v_load_interval interval;
	v_cnt_prt int8;
	v_cnt int8;

begin	
	select coalesce('with ('||array_to_string(reloptions, ', ')||')','')
	from pg_class into v_params
	where oid = 'std5_80.stores_mart'::regclass;
	
	v_load_interval = '3 month'::interval;
	v_start_date = date_trunc('month', p_start_date);
	v_end_date = date_trunc('month', p_end_date);
	v_iter_date = v_start_date;
	v_cnt = 0;

	while v_iter_date <= v_end_date loop	
		drop table if exists std5_80.stores_mart_tmp;
		v_sql = 'create table std5_80.stores_mart_tmp (like std5_80.stores_mart) '||v_params||' distributed randomly;';
	
		raise notice 'Temp table is: %', v_sql;
		
		execute v_sql;		
		
		insert into std5_80.stores_mart_tmp
		with m_sales as
		(select 
			plant,
			bh.calday,
			sum(rpa_sat) revenue,
    		sum(bi.qty) sold_qty,
		    count(distinct billnum) bills_qty
		from std5_80.bills_head bh
			join std5_80.bills_item bi using(billnum)
		where bi.calday >= v_iter_date and bi.calday < v_iter_date + v_load_interval
		group by plant, bh.calday),
		m_promos as
		(select
			plant,
			calday,
			sum(case 
		        when p."type" = 1 then discount
		        when p."type" = 2 then (discount * 0.01) * b.price
		        else 0
		    end) coupon_disc,
		    count(*) promo_qty
		from (select distinct billnum, material, calday, rpa_sat/qty price from std5_80.bills_item) b
			join std5_80.coupons c using(billnum, material)
			join std5_80.promos p using(promo_id)
		where "date" >= v_iter_date and "date" < v_iter_date + v_load_interval
		group by plant, calday),
		m_traffic as
		(select 
			plant, 
			date as calday,
			sum(quantity) traffic
		from std5_80.traffic
		where date >= v_iter_date and date < v_iter_date + v_load_interval
		group by plant, calday)
		select 
			plant,
			calday,
			revenue,
			coupon_disc,
			sold_qty,
			bills_qty,
			traffic,
			promo_qty
		from m_traffic
			left join m_sales using(plant, calday)
			left join m_promos using(plant, calday);
		
		get diagnostics v_cnt_prt = ROW_COUNT;
		raise notice 'Inserted rows: %', v_cnt_prt;
		
		v_sql = 'alter table std5_80.stores_mart exchange partition for (date '''||v_iter_date||''') with table std5_80.stores_mart_tmp with validation';
		
		raise notice 'Exchange partition script: %', v_sql;
		
		execute v_sql;
		
		v_cnt = v_cnt + v_cnt_prt;
		v_iter_date = v_iter_date + v_load_interval;
	end loop;
	
	return v_cnt;
end;

$$
EXECUTE ON ANY;