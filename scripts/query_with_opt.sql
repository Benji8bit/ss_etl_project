-- New script in adb.
-- Date: Jan 22, 2024
-- Time: 4:42:34 AM
-------------------------------

Gather Motion 6:1  (slice1; segments: 6)  (cost=0.00..187382.87 rows=790587654 width=66) (actual time=284.985..6729.024 rows=11197440 loops=1)
  ->  Hash Join  (cost=0.00..32759.74 rows=131764609 width=66) (actual time=296.364..2573.156 rows=2972160 loops=1)
        Hash Cond: (spotify_songs.track_id = spotify_songs_more_artists.track_id)
        Extra Text: (seg4)   Hash chain length 177.2 avg, 675 max, using 16 of 262144 buckets.
        ->  Seq Scan on spotify_songs  (cost=0.00..756.14 rows=2801750 width=48) (actual time=0.722..986.420 rows=2856448 loops=1)
        ->  Hash  (cost=531.05..531.05 rows=209509 width=41) (actual time=294.161..294.161 rows=2835 loops=1)
              ->  Seq Scan on spotify_songs_more_artists  (cost=0.00..531.05 rows=209509 width=41) (actual time=0.662..293.271 rows=2835 loops=1)
                    Filter: ((track_artist = 'Sia'::text) OR (((duration_ms)::integer >= 299900) AND ((duration_ms)::integer <= 300827)))
Planning time: 52.662 ms
  (slice0)    Executor memory: 856K bytes.
  (slice1)    Executor memory: 3457K bytes avg x 6 workers, 3676K bytes max (seg4).  Work_mem: 188K bytes max.
Memory used:  128000kB
Optimizer: Pivotal Optimizer (GPORCA)
Execution time: 7411.506 ms