-- New script in adb.
-- Date: Jan 22, 2024
-- Time: 4:26:55 AM
-------------------------------

Gather Motion 6:1  (slice1; segments: 6)  (cost=0.00..198477.13 rows=792082695 width=66) (actual time=126.030..7800.860 rows=11197440 loops=1)
  ->  Append  (cost=0.00..43561.60 rows=132013783 width=66) (actual time=160.044..4363.977 rows=2972160 loops=1)
        ->  Hash Join  (cost=0.00..2192.04 rows=569779 width=66) (actual time=160.038..2173.208 rows=2004480 loops=1)
              Hash Cond: (spotify_songs.track_id = spotify_songs_more_artists.track_id)
              Extra Text: (seg1)   Hash chain length 243.0 avg, 405 max, using 5 of 131072 buckets.
              Extra Text: (seg4)   Hash chain length 243.0 avg, 675 max, using 5 of 131072 buckets.
              ->  Seq Scan on spotify_songs  (cost=0.00..756.14 rows=2801750 width=48) (actual time=0.640..969.352 rows=2856448 loops=1)
              ->  Hash  (cost=484.36..484.36 rows=906 width=41) (actual time=128.765..128.765 rows=1215 loops=1)
                    ->  Seq Scan on spotify_songs_more_artists  (cost=0.00..484.36 rows=906 width=41) (actual time=12.360..128.414 rows=1215 loops=1)
                          Filter: (track_artist = 'Sia'::text)
        ->  Hash Join  (cost=0.00..32656.65 rows=131444004 width=66) (actual time=213.548..1800.039 rows=1036800 loops=1)
              Hash Cond: (spotify_songs_1.track_id = spotify_songs_more_artists_1.track_id)
              Extra Text: (seg2)   Hash chain length 146.2 avg, 270 max, using 12 of 131072 buckets.
              ->  Seq Scan on spotify_songs spotify_songs_1  (cost=0.00..756.14 rows=2801750 width=48) (actual time=0.604..928.349 rows=2856448 loops=1)
              ->  Hash  (cost=502.54..502.54 rows=208999 width=41) (actual time=212.752..212.752 rows=1755 loops=1)
                    ->  Seq Scan on spotify_songs_more_artists spotify_songs_more_artists_1  (cost=0.00..502.54 rows=208999 width=41) (actual time=3.487..212.246 rows=1755 loops=1)
                          Filter: (((duration_ms)::integer >= 299900) AND ((duration_ms)::integer <= 300827))
Planning time: 23.117 ms
  (slice0)    Executor memory: 1496K bytes.
  (slice1)    Executor memory: 3152K bytes avg x 6 workers, 3236K bytes max (seg4).  Work_mem: 119K bytes max.
Memory used:  128000kB
Optimizer: Pivotal Optimizer (GPORCA)
Execution time: 8442.637 ms