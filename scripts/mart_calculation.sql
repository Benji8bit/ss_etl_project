-- New script in adb.
-- Date: Jan 29, 2024
-- Time: 6:33:48 AM
-------------------------------

SELECT *
FROM std5_80.plan
--LIMIT 10;

SELECT *
FROM std5_80.sales
--LIMIT 10;

SELECT DISTINCT "date" 
FROM std5_80.sales;

-----------------Расчёт кода самого продаваемого товара в регионе--------------------------------------------------

SELECT region, material, sum_qty 
FROM ( SELECT region, material, sum_qty, ROW_NUMBER() OVER(PARTITION BY region ORDER BY sum_qty DESC) as rn
FROM (
	SELECT region, material, SUM(quantity) AS sum_qty
	FROM std5_80.sales s 
	GROUP BY region, material 
	ORDER BY region ASC, sum_qty DESC
	) AS t1
) AS t2
WHERE rn = 1
ORDER BY region

-----------------Расчёт среднего процента продаж за месяц---------------------------------------------------------

SELECT DATE_TRUNC('month', t.date) AS all_month, AVG(fact_percent), ROUND(SUM(t.fact_qty)/SUM(t.plan_qty)*100, 2)
FROM (
	SELECT p."date" , p.region, p.matdirec, p.distr_chan, 
		   SUM(p.quantity) AS plan_qty, SUM(s.quantity) AS fact_qty,  
		   ROUND((SUM(s.quantity)::NUMERIC/SUM(p.quantity)::NUMERIC)*100, 2) AS fact_percent
	FROM std5_80.plan p 
	JOIN std5_80.sales s ON p."date"  = s."date"  
	WHERE p."date" BETWEEN date_trunc('month', to_date('202102', 'YYYYMM')) - INTERVAL '1 month'
				   AND date_trunc('month', to_date('202103', 'YYYYMM'))
	GROUP BY p."date", p.region, p.matdirec, p.distr_chan 
	ORDER BY p."date") AS t
GROUP BY 1;

---------------Расчёт витрины-------------------------------------------------------------------------------------

SELECT p.region, p.matdirec, p.distr_chan, 
		   SUM(p.quantity) AS plan_qty, SUM(s.quantity) AS fact_qty,  
		   ROUND((SUM(s.quantity)::NUMERIC/SUM(p.quantity)::NUMERIC)*100, 2) AS fact_percent
	FROM std5_80.plan p 
	JOIN std5_80.sales s ON p."date"  = s."date"  
	WHERE p."date" BETWEEN date_trunc('month', to_date('202102', 'YYYYMM')) - INTERVAL '1 month'
				   AND date_trunc('month', to_date('202103', 'YYYYMM'))
	GROUP BY p.region, p.matdirec, p.distr_chan 
	ORDER BY p.region, p.matdirec, p.distr_chan
	
	-----Соединение таблиц среднего процента продаж по региону с самым продаваемым товаром----------------

--EXPLAIN ANALYZE 
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
WHERE p."date" BETWEEN date_trunc('month', to_date('202107', 'YYYYMM')) - INTERVAL '3 month'
			   AND date_trunc('month', to_date('202107', 'YYYYMM'))			   
GROUP BY p.region, p.matdirec, p.distr_chan, t.material
ORDER BY 1, 2, 3
