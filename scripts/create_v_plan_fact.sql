-- New script in adb.
-- Date: Jan 31, 2024
-- Time: 1:53:15 AM
-------------------------------

-----------------------Соединение представления на основе созданной ранее витрины-------------------------------

/*
 3. Создайте представление (VIEW) на созданной ранее витрине в схеме std <номер студента> со следующим набором полей:

Код "Региона".
Текст "Региона".
Код "Товарного направления" (matdirec).
Код "Канала сбыта".
Текст "Канала сбыта".
Процент выполнения плана за месяц.
Код самого продаваемого товара в регионе.
Код "Бренда" самого продаваемого товара в регионе.
Текст самого продаваемого товара в регионе.
Цена самого продаваемого товара в регионе.

Название представления v_plan_fact. Создание представления можно встроить в функцию по расчёту витрины. 
*/

SELECT *
FROM std5_80.plan p 
LIMIT 10

SELECT *
FROM std5_80.sales s  
LIMIT 10

SELECT *
FROM std5_80.region r 
LIMIT 10

SELECT *
FROM std5_80.chanel c  
LIMIT 10

SELECT *
FROM std5_80.product p2 
LIMIT 10

SELECT * 
FROM std5_80.price p 
LIMIT 10

DROP VIEW std5_80.v_plan_fact;

SELECT *
FROM std5_80.v_plan_fact;


--EXPLAIN ANALYZE 
CREATE OR REPLACE VIEW std5_80.v_plan_fact AS
	SELECT DISTINCT p.region, r.txt AS "region_name", p.matdirec, s.distr_chan, c.txtsh AS "chanel_name", 
		   SUM(p.quantity) AS plan_qty, SUM(s.quantity) AS fact_qty,  
		   ROUND((SUM(s.quantity)::NUMERIC/SUM(p.quantity)::NUMERIC)*100, 2) AS sales_percent, 
		   t.material AS bestseller, p2.brand, p2.txt AS "product_description", p3.price  
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
	JOIN std5_80.region r ON p.region = r.region 
	JOIN std5_80.chanel c ON s.distr_chan = c.distr_chan 
	JOIN std5_80.product p2 ON t.material = p2.material::varchar 
	JOIN std5_80.price p3 ON t.material = p3.material::varchar
	WHERE p."date" BETWEEN date_trunc('month', to_date('202103', 'YYYYMM')) - INTERVAL '1 month'
				   AND date_trunc('month', to_date('202103', 'YYYYMM'))			   
	GROUP BY p.region, p.matdirec, s.distr_chan, c.txtsh, r.txt, t.material, p2.brand, p2.txt, p3.price  
	ORDER BY 1, 2, 3, 4