-- New script in adb.
-- Date: Jan 28, 2024
-- Time: 2:39:33 AM
-------------------------------

SELECT std5_80.f_load_full('std5_80.region', 'region');

SELECT std5_80.f_load_full('std5_80.chanel', 'chanel');

SELECT std5_80.f_load_full('std5_80.price', 'price');

SELECT std5_80.f_load_full('std5_80.product', 'product');

SELECT *
FROM std5_80.region;

SELECT *
FROM std5_80.chanel;

SELECT *
FROM std5_80.price;

SELECT *
FROM std5_80.product;