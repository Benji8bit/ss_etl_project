-- New script in adb.
-- Date: Jan 18, 2024
-- Time: 2:08:38 AM
-------------------------------

CREATE SCHEMA std5_80;

--CREATE SCHEMA ynwa_2024;

SET SEARCH_PATH TO std5_80;

--SET SEARCH_PATH TO ynwa_2024;

DROP SCHEMA ynwa_2024;

CREATE TABLE std5_80.table1
(
field1 INT,
field2 TEXT
)
DISTRIBUTED BY (field1);

--DROP TABLE std5_80.table_1;

INSERT INTO std5_80.table1
SELECT a, md5(a::TEXT)
FROM GENERATE_SERIES(1, 1000) a;

SELECT gp_segment_id, COUNT(1)
FROM std5_80.table1
GROUP BY 1
ORDER BY 1;

