-- New script in adb.
-- Date: Feb 23, 2024
-- Time: 12:49:50 AM
----------------------------------------------------------------------------------------------------------------------------------------

--Здесь адаптируем проверяем код запроса для дальнейшего построения датасета в Apache Superset

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
    round(sum(stores_mart.revenue) / sum(stores_mart.traffic)::numeric, 2) AS "Средняя выручка на одного посетителя"
   FROM std5_80.stores_mart
   JOIN std5_80.stores USING (plant)
  GROUP BY stores_mart.plant, stores.txt
  ORDER BY stores_mart.plant;
  
 SELECT *
 FROM std5_80.v_stores_mart;
 
SELECT SUM(stores_mart.revenue)
FROM std5_80.stores_mart