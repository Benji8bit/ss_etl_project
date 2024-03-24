-- New script in adb.
-- Date: Jan 22, 2024
-- Time: 4:15:40 AM
-------------------------------

SET SEARCH_PATH TO homework_3;

--1. Первая оптимизация запроса через замену функции на простой фильтр

EXPLAIN ANALYZE
SELECT SUM(re.sale_price)
FROM homework_3.real_estate re 
WHERE date_trunc('year',"date") = '2009-01-01';

EXPLAIN ANALYZE
SELECT SUM(re.sale_price)
FROM homework_3.real_estate re 
WHERE "date" = '2009-01-01';

--2. Вторая оптимизация запроса (также через оптиимзацию фильтра)

-- Первая версия запроса: 
-- Gather Motion 6:1  (slice1; segments: 6)  (cost=0.00..198477.13 rows=792082695 width=66) (actual time=126.030..7800.860 rows=11197440 loops=1)
-- Execution time: 8442.637 ms

SELECT s.track_id,
	a.track_name,
	track_album_id,
	track_popularity
FROM homework_3.spotify_songs_more_artists a 
JOIN homework_3.spotify_songs s 
	ON a.track_id = s.track_id
WHERE track_artist = 'Sia'
UNION ALL
SELECT s.track_id,
       a.track_name,
       track_album_id,
       track_popularity
FROM homework_3.spotify_songs_more_artists a
JOIN homework_3.spotify_songs s 
	ON a.track_id = s.track_id
WHERE duration_ms::int between 299900 and 300827

-- Оптимизированная версия запроса
-- Gather Motion 6:1  (slice1; segments: 6)  (cost=0.00..187382.87 rows=790587654 width=66) (actual time=284.985..6729.024 rows=11197440 loops=1)
-- Execution time: 7411.506 ms

EXPLAIN ANALYZE
SELECT s.track_id,
	a.track_name,
	track_album_id,
	track_popularity
FROM homework_3.spotify_songs_more_artists a 
JOIN homework_3.spotify_songs s 
	ON a.track_id = s.track_id
WHERE track_artist = 'Sia' OR duration_ms::int between 299900 and 300827
