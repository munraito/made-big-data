SELECT `date`, COUNT(*) as cnt
FROM logs
GROUP BY `date`
SORT BY cnt DESC
LIMIT 10;
