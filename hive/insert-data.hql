INSERT OVERWRITE TABLE bd_prueba.ut_data_partitioned PARTITION(year, month, day) 
SELECT *, 
       year(from_unixtime(timestamp)) AS year,
       month(from_unixtime(timestamp)) AS month,
       day(from_unixtime(timestamp)) AS day
FROM bd_prueba.ut_data;