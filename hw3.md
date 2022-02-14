## Homework
### Question 1: 
**What is count for fhv vehicles data for year 2019**  
Can load the data for cloud storage and run a count(*)
```
SELECT COUNT(*) AS cnt FROM trips_data_all.fhv_tripdata
WHERE EXTRACT(YEAR FROM pickup_datetime) = 2019;
```
### Question 2: 
**How many distinct dispatching_base_num we have in fhv for 2019**  
Can run a distinct query on the table from question 1
```
SELECT COUNT(DISTINCT dispatching_base_num) AS num_dist_dispatching_base FROM trips_data_all.fhv_tripdata;
```

### Question 4: 
**What is the count, estimated and actual data processed for query which counts trip between 2019/01/01 and 2019/03/31 for dispatching_base_num B00987, B02060, B02279**  
Create a table with optimized clustering and partitioning, and run a 
count(*). Estimated data processed can be found in top right corner and
actual data processed can be found after the query is executed.
```
SELECT COUNT(*) FROM trips_data_all.fhv_tripdata
WHERE DATE(pickup_datetime) BETWEEN '2019-01-01' AND '2019-03-31'
AND dispatching_base_num IN ('B00987', 'B02060', 'B02279');
```
```
SELECT COUNT(*) FROM trips_data_all.fhv_tripdata
WHERE dropoff_datetime BETWEEN '2019-01-01' AND '2019-03-31'
AND dispatching_base_num IN ('B00987', 'B02060', 'B02279');
```
