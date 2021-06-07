ADD JAR /usr/local/hive/lib/hive-serde.jar;

--ip table
DROP TABLE IF EXISTS ip_regions;

CREATE EXTERNAL TABLE ip_regions ( 
	ip STRING,
 	region STRING
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t'
STORED AS textfile
LOCATION '/data/user_logs/ip_data_M';

SELECT * FROM ip_regions LIMIT 10;

--users table
DROP TABLE IF EXISTS users;

CREATE EXTERNAL TABLE users ( 
	ip STRING,
 	browser STRING,
	sex STRING,
	age INT
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t'
STORED AS textfile
LOCATION '/data/user_logs/user_data_M';

SELECT * FROM users LIMIT 10;

--logs table
DROP TABLE IF EXISTS logs_raw;

CREATE EXTERNAL TABLE logs_raw (
	ip STRING,
	`date` STRING,
	request STRING,
	page_size INT,
	http_status INT,
	user_agent STRING
) 
ROW FORMAT
	serde 'org.apache.hadoop.hive.serde2.RegexSerDe'
	with serdeproperties (
		"input.regex" = "^(\\S*)\\s*(\\S*)\\s*(\\S*)\\s*(\\S*)\\s*(\\S*)\\s*(\\S*)\\s.*" 
	)
STORED AS textfile
LOCATION '/data/user_logs/user_logs_M';

SELECT * FROM logs_raw LIMIT 10;

--SELECT COUNT(DISTINCT SUBSTR(`date`,1,8)) from logs_raw;
-- partitioning
set hive.exec.max.dynamic.partitions.pernode=116;
set hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE IF EXISTS logs;

CREATE EXTERNAL TABLE logs (
        ip STRING,
        request STRING,
        page_size INT,
        http_status INT,
        user_agent STRING
)  PARTITIONED BY ( `date` STRING );
INSERT OVERWRITE TABLE logs PARTITION(`date`) SELECT ip, request, page_size, http_status, user_agent, SUBSTR(`date`,1,8) as `date` FROM logs_raw;

SELECT * FROM logs LIMIT 10;
