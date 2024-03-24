-- New script in INTERN Clickhouse 206.
-- Date: Feb 23, 2024
-- Time: 7:23:44 PM
-------------------------------

SELECT `plant` AS `Завод`,
       `plant_name` AS `Наименование завода`,
       sum(`revenue`) AS `Оборот`,
       sum(`coupon_disc`) AS `Скидки по купонам`,
       sum(revenue) - sum(coupon_disc) AS `Оборот с учетом скидки`,
       sum(`sold_qty`) AS `Кол-во проданных товаров`,
       sum(`bills_qty`) AS `Количество чеков`,
       sum(`traffic`) AS `Трафик`,
       sum(`promo_qty`) AS `Кол-во товаров по акции`,
       sum(promo_qty) / sum(sold_qty) AS `Доля товаров со скидкой`,
       sum(sold_qty) / sum(bills_qty) AS `Среднее количество товаров в чеке`,
       sum(bills_qty) / sum(traffic) AS `Коэффициент конверсии магазина`,
       sum(revenue) / sum(bills_qty) AS `Средний чек, руб`,
       sum(revenue) / sum(traffic) AS `Средняя выручка на одного посетит`
FROM
  (SELECT dictGet('std5_80.ch_stores_dict', 'txt', plant) AS plant_name,
          *
   FROM std5_80.ch_stores_mart_distr) AS `virtual_table`
GROUP BY `plant`,
         `plant_name`
ORDER BY plant ASC
LIMIT 100;