-- New script in adb.
-- Date: Jan 30, 2024
-- Time: 6:46:41 AM
----------------------------------------------------------------

/*
2. Создайте пользовательскую функцию в схеме std <номер студента> для расчёта витрины, которая будет содержать результат выполнения плана продаж в разрезе: 
Код "Региона".
Код "Товарного направления" (matdirec).
Код "Канала сбыта".
Плановое количество.
Фактические количество.
Процент выполнения плана за месяц.
Код самого продаваемого товара в регионе*.
Требования к функции по расчёту витрины:

Функция должна принимать на вход месяц, по которому будут вестись расчеты. 
Таблица должна формироваться в схеме std <номер студента>.
Название таблицы должно формироваться по шаблону plan_fact_<YYYYMM>, где <YYYYMM> - месяц расчета. 
Функция должна иметь возможность безошибочного запуска несколько раз по одному и тому же месяцу. 
Необязательно рассчитывать витрины для каждого месяца. Для тренировки можно рассчитать один месяц и далее работать с ним в других ДЗ.
*/

----------------------------------------------------------------

CREATE OR REPLACE FUNCTION std5_80.f_load_mart(p_month VARCHAR)
	RETURNS INT4
	LANGUAGE plpgsql
	VOLATILE
AS $$

DECLARE 
	v_table_name TEXT := 'std5_80.plan_fact_'||p_month;
	v_sql TEXT;
	v_return INT;

BEGIN
	
	PERFORM std5_80.f_load_write_log(p_log_type := 'INFO',
									 p_log_message := 'Start f_load_mart',
									 p_location := 'Sales mart calculation');
									
	v_sql = 'DROP TABLE IF EXISTS '||v_table_name||';
			 CREATE TABLE '||v_table_name||'
				WITH (
					appendonly=TRUE,
					orientation=COLUMN,
					compresstype=zstd,
					compresslevel=1)
				AS 
					SELECT DISTINCT p.region, p.matdirec, p.distr_chan,
						   SUM(p.quantity) AS plan_qty, SUM(s.quantity) AS fact_qty,  
						   ROUND((SUM(s.quantity)::NUMERIC/SUM(p.quantity)::NUMERIC)*100, 2) AS sales_percent, 
						   t.material AS bestseller
					FROM std5_80.plan p 
					JOIN std5_80.sales s ON p."date"  = s."date" 
					LEFT JOIN (
						SELECT region, material, sum_qty 
						FROM ( SELECT region, material, sum_qty, ROW_NUMBER() OVER(PARTITION BY region ORDER BY sum_qty DESC) as rn
						FROM (
							SELECT region, material, SUM(quantity) AS sum_qty
							FROM std5_80.sales s 
							GROUP BY region, material) AS t1
						) AS t2
						WHERE rn = 1) AS t ON t.region = p.region 
					WHERE p."date" BETWEEN date_trunc(''month'', to_date('''||p_month||''', ''YYYYMM'')) - INTERVAL ''1 month''
								   AND date_trunc(''month'', to_date('''||p_month||''', ''YYYYMM''))			   
					GROUP BY p.region, p.matdirec, p.distr_chan, t.material
					ORDER BY 1, 2, 3
					DISTRIBUTED RANDOMLY;';
	
	RAISE NOTICE 'LOAD_MART TABLE IS: %', v_sql;

	EXECUTE v_sql;
		
	EXECUTE 'SELECT count(*) FROM '||v_table_name INTO v_return;

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