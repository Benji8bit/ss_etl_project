-- New script in adb.
-- Date: Feb 22, 2024
-- Time: 7:37:58 PM
-------------------------------
SELECT * FROM std5_80.bills_item;
SELECT * FROM std5_80.coupons c ;


INSERT INTO std5_80.stores_mart_tmp
		WITH m_sales AS
		(SELECT 
			plant,
			bh.calday,
			SUM(rpa_sat) revenue,
    		SUM(bi.qty) sold_qty,
		    COUNT(DISTINCT bh.billnum) bills_qty
		FROM std5_80.bills_head bh
		JOIN std5_80.bills_item bi ON bh.billnum = bi.billnum
		WHERE bi.calday >= '2021-01-01'::date AND bi.calday < '2021-03-01'::date-- + '1 month'
		GROUP BY plant, bh.calday),
		m_promos AS
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
		WHERE "date" >= '2021-01-01' AND "date" < '2021-01-01' + '1 month'
		GROUP BY plant, calday),
		m_traffic AS
		(SELECT 
			plant, 
			date AS calday,
			SUM(quantity) traffic
		FROM std5_80.traffic
		WHERE date >= '2021-01-01' AND date < '2021-01-01' + '1 month'
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
		FROM m_traffic
			LEFT JOIN m_sales USING(plant, calday)
			LEFT JOIN m_promos USING(plant, calday);