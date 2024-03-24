

CREATE EXTENSION IF NOT EXISTS pxf;

DROP EXTERNAL TABLE std5_75.plan_ext;

CREATE EXTERNAL TABLE std5_75.plan_ext (
	"date" date ,
	region varchar(20) ,
	matdirec varchar(20) ,
	quantity int4 ,
	distr_chan varchar(100) 
	
)
LOCATION ('pxf://gp.plan?PROFILE=JDBC
			&JDBC_DRIVER=org.postgresql.Driver
			&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres
			&USER=intern
			&PASS=intern'
)ON ALL
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import')
ENCODING 'UTF8'


SELECT * FROM std5_75.plan_ext
LIMIT 10

DROP EXTERNAL TABLE std5_75.price_ext;

CREATE EXTERNAL TABLE std5_75.price_ext (
	material int4 ,
	region varchar(4) ,
	distr_chan varchar(1) ,
	price int4  
	
)
LOCATION ('pxf://gp.price?PROFILE=JDBC
			&JDBC_DRIVER=org.postgresql.Driver
			&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres
			&USER=intern
			&PASS=intern'
)ON ALL
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import')
ENCODING 'UTF8'