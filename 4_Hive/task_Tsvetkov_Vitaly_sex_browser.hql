set hive.auto.convert.join = false;
set mapreduce.job.reduces = 8;

SELECT browser, SUM(IF(sex = 'male', 1, 0)) as male_cnt,
 SUM(IF(sex = 'female', 1, 0)) as female_cnt
FROM logs JOIN users
ON (logs.ip = users.ip)
GROUP BY browser
LIMIT 10;
