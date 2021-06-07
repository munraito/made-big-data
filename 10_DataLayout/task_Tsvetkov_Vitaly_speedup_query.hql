select count(*) from ${table_name};
select avg(page_size) from ${table_name};
select max(page_size) from ${table_name} where http_status=404; 
select min(page_size) from ${table_name} where http_status=200; 
