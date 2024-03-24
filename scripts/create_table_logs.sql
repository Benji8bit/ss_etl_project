-- New script in adb.
-- Date: Jan 29, 2024
-- Time: 12:54:46 AM
-------------------------------

CREATE TABLE std5_80.logs (
	log_id INT8 NOT NULL,
	log_timestamp TIMESTAMP NOT NULL DEFAULT now(),
	log_type TEXT NOT NULL,
	log_msg TEXT NOT NULL,
	log_location TEXT NULL,
	is_error BOOL NULL,
	log_user TEXT NULL DEFAULT "current_user"(),
	CONSTRAINT pk_log_id PRIMARY KEY (log_id)
)
DISTRIBUTED BY (log_id);