-- New script in adb.
-- Date: Feb 20, 2024
-- Time: 2:53:09 AM
-------------------------------

CREATE OR REPLACE VIEW std5_80.v_stores_mart
AS SELECT stores_mart.plant AS "Завод (Код)",
    stores.txt AS "Завод (Текст)",
    sum(stores_mart.revenue) AS "Оборот",
    sum(stores_mart.coupon_disc) AS "Скидки по купонам",
    sum(stores_mart.revenue) - sum(stores_mart.coupon_disc) AS "Оборот с учетом скидки",
    sum(stores_mart.sold_qty) AS "Кол-во проданных товаров",
    sum(stores_mart.bills_qty) AS "Количество чеков",
    sum(stores_mart.traffic) AS "Трафик",
    sum(stores_mart.promo_qty) AS "Кол-во товаров по акции",
    round(100.0 * sum(stores_mart.promo_qty)::numeric / sum(stores_mart.sold_qty)::numeric, 2) || '%'::text AS "Доля товаров со скидкой",
    round(sum(stores_mart.sold_qty)::numeric / sum(stores_mart.bills_qty)::numeric, 2) AS "Среднее количество товаров в чеке",
    round(100.0 * sum(stores_mart.bills_qty)::numeric / sum(stores_mart.traffic)::numeric, 2) || '%'::text AS "Коэффициент конверсии магазина, %",
    round(sum(stores_mart.revenue) / sum(stores_mart.bills_qty)::numeric, 2) AS "Средний чек, руб",
    round(sum(stores_mart.revenue) / sum(stores_mart.traffic)::numeric, 2) AS "Средняя выручка на одного посетит"
   FROM std5_34.stores_mart
     JOIN std5_34.stores USING (plant)
  GROUP BY stores_mart.plant, stores.txt
  ORDER BY stores_mart.plant;
  
 -----------------------------------------------------------------------
 
 CREATE TABLE std5_80.stores_mart_tmp (
	plant bpchar(4) NULL,
	calday date NULL,
	revenue numeric(9, 2) NULL,
	coupon_disc numeric(9, 2) NULL,
	sold_qty int4 NULL,
	bills_qty int4 NULL,
	traffic int4 NULL,
	promo_qty int4 NULL,
	CONSTRAINT stores_mart_1_prt_2_check CHECK (((calday >= '2021-01-01'::date) AND (calday < '2021-04-01'::date)))
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED RANDOMLY;

-----------------------------------------------------------------------

CREATE TABLE std5_80.stores_mart (
	plant bpchar(4) NULL,
	calday date NULL,
	revenue numeric(9, 2) NULL,
	coupon_disc numeric(9, 2) NULL,
	sold_qty int4 NULL,
	bills_qty int4 NULL,
	traffic int4 NULL,
	promo_qty int4 NULL
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED RANDOMLY;

----------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION std5_80.f_load_delta_partition(p_table text, p_partition_key text, p_start_date timestamp, p_end_date timestamp)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
AS $$	
	
declare
	v_ext_table text;
	v_temp_table text;
	v_sql text;
	v_dist_key text;
	v_params text;
	v_where text;
	v_start_date date;
	v_end_date date;
	v_iter_date date;
	v_load_interval interval;
	v_table_oid int4;
	v_cnt_prt int8;
	v_cnt int8;
	v_columns text;

begin
	v_ext_table = p_table||'_ext';
	v_temp_table = p_table||'_tmp';
	select c.oid into v_table_oid
	from pg_class c join pg_namespace n on c.relnamespace = n.oid
	where n.nspname||'.'||c.relname = p_table
	limit 1;
	
	if v_table_oid = 0 or v_table_oid is null then 
		v_dist_key = 'distributed randomly';
	else
		v_dist_key = pg_get_table_distributedby(v_table_oid);
	end if;
	
	select coalesce('with ('||array_to_string(reloptions, ', ')||')','')
	from pg_class into v_params
	where oid = p_table::regclass;
	
	set DateStyle to 'ISO, DMY';
	
	select string_agg(
	case 
        when attname = p_partition_key then p_partition_key||'::date'
        else attname
    end, ', ') into v_columns
	from pg_catalog.pg_attribute
	where attrelid = p_table::regclass
	and attnum > 0
	and not attisdropped;
	
	v_load_interval = '1 month'::interval;
	v_start_date = date_trunc('month', p_start_date);
	v_end_date = date_trunc('month', p_end_date);
	v_iter_date = v_start_date;
	v_cnt = 0;

	while v_iter_date <= v_end_date loop		
		v_sql = 'drop table if exists '||v_temp_table||';
			create table '||v_temp_table||' (like '||p_table||') '||v_params||' '||v_dist_key||';';
	
		raise notice 'Temp table is: %', v_sql;
		
		execute v_sql;
		
		v_where = p_partition_key||'::date >= '''||v_iter_date||'''::date and '||p_partition_key||'::date < '''||v_iter_date + v_load_interval ||'''::date';
		v_sql = 'insert into '||v_temp_table||' select '||v_columns||' from '||v_ext_table||' where '||v_where;
		
		execute v_sql;
		
		get diagnostics v_cnt_prt = ROW_COUNT;
		raise notice 'Inserted rows: %', v_cnt_prt;
		
		v_sql = 'alter table '||p_table||' exchange partition for (date '''||v_iter_date||''') with table '||v_temp_table||' with validation';
		
		raise notice 'Exchange partition script: %', v_sql;
		
		execute v_sql;
		
		v_cnt = v_cnt + v_cnt_prt;
		v_iter_date = v_iter_date + v_load_interval;
	end loop;
	
	return v_cnt;
end;

$$
EXECUTE ON ANY;

-----------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION std5_80.f_load_full(p_table text, p_file_name text)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
AS $$	

declare
	v_ext_table_name text;
	v_sql text;
	v_gpfdist text;
	v_result int4;

begin
	v_ext_table_name = p_table||'_ext';
	execute 'truncate table '||p_table;
	execute 'drop external table if exists '||v_ext_table_name;
	v_gpfdist = 'gpfdist://172.16.128.58:8080/'||p_file_name||'.csv';
	v_sql = 'create external table '||v_ext_table_name||' (like '||p_table||')
			location ('''||v_gpfdist||''') on all
			format ''CSV'' (delimiter '';'' null '''' escape ''"'' quote ''"'' header)
			encoding ''UTF8''
			segment reject limit 10 rows';
	raise notice 'externaal tble is: %', v_sql;
	execute v_sql;
	execute 'insert into '||p_table||' select * from '||v_ext_table_name;
	execute 'select count(*) from '||p_table into v_result;
	return v_result;
end;

$$
EXECUTE ON ANY;

----------------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION std5_80.f_load_mart(p_start_date timestamp, p_end_date timestamp)
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
	where oid = 'std5_34.stores_mart'::regclass;
	
	v_load_interval = '3 month'::interval;
	v_start_date = date_trunc('month', p_start_date);
	v_end_date = date_trunc('month', p_end_date);
	v_iter_date = v_start_date;
	v_cnt = 0;

	while v_iter_date <= v_end_date loop	
		drop table if exists std5_34.stores_mart_tmp;
		v_sql = 'create table std5_34.stores_mart_tmp (like std5_34.stores_mart) '||v_params||' distributed randomly;';
	
		raise notice 'Temp table is: %', v_sql;
		
		execute v_sql;		
		
		insert into std5_34.stores_mart_tmp
		with m_sales as
		(select 
			plant,
			bh.calday,
			sum(rpa_sat) revenue,
    		sum(bi.qty) sold_qty,
		    count(distinct billnum) bills_qty
		from std5_34.bills_head bh
			join std5_34.bills_item bi using(billnum)
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
		from (select distinct billnum, material, calday, rpa_sat/qty price from std5_34.bills_item) b
			join std5_34.coupons c using(billnum, material)
			join std5_34.promos p using(promo)
		where "date" >= v_iter_date and "date" < v_iter_date + v_load_interval
		group by plant, calday),
		m_traffic as
		(select 
			plant, 
			date as calday,
			sum(quantity) traffic
		from std5_34.traffic
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
		
		v_sql = 'alter table std5_34.stores_mart exchange partition for (date '''||v_iter_date||''') with table std5_34.stores_mart_tmp with validation';
		
		raise notice 'Exchange partition script: %', v_sql;
		
		execute v_sql;
		
		v_cnt = v_cnt + v_cnt_prt;
		v_iter_date = v_iter_date + v_load_interval;
	end loop;
	
	return v_cnt;
end;

$$
EXECUTE ON ANY;

