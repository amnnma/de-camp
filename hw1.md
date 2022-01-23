## Week 1 Homework


## Question 1. Google Cloud SDK

```
Google Cloud SDK 368.0.0 
alpha 2022.01.07 
beta 2022.01.07 
bq 2.0.72 
core 2022.01.07 
gsutil 5.6
```


## Question 2. Terraform 
```
google_bigquery_dataset.dataset: Creating...
google_storage_bucket.data-lake-bucket: Creating...
google_bigquery_dataset.dataset: Creation complete after 3s [id=projects/bp-de-338615/datasets/trips_data_all]
google_storage_bucket.data-lake-bucket: Creation complete after 4s [id=dtc_data_lake_bp-de-338615]

Apply complete! Resources: 2 added, 0 changed, 0 destroyed.
```


## Question 3. Count records 

```
select
    CAST(tpep_pickup_datetime AS DATE) as "day",
    COUNT(1) as "count"
from yellow_taxi_trips
where date_part('day', tpep_pickup_datetime) = 15
group by CAST(tpep_pickup_datetime AS DATE)
ORDER BY "count" DESC;
```

## Question 4. Average

```
select
CAST(tpep_pickup_datetime AS DATE) as "day",
MAX(tip_amount) as "count"
from yellow_taxi_trips
group by CAST(tpep_pickup_datetime AS DATE)
ORDER BY "count" DESC
limit 1;
```

## Question 5. Most popular destination

```
SELECT 
	CAST(tpep_pickup_datetime AS DATE) as "day",
	CONCAT(zpu."Borough", ' / ', zpu."Zone") AS "pick_up_loc",
	CONCAT(zdo."Borough", ' / ', zdo."Zone") AS "dropoff_loc",
	COUNT(*)
FROM
	yellow_taxi_trips t JOIN zones zpu
		ON t."PULocationID" = zpu."LocationID"
	JOIN zones zdo
		ON t."DOLocationID" = zdo."LocationID"
where date_part('day', tpep_pickup_datetime) = 14 and date_part('month', tpep_pickup_datetime) = 1 and  CONCAT(zpu."Borough", ' / ', zpu."Zone")='Manhattan / Central Park'
group  by CAST(tpep_pickup_datetime AS DATE),CONCAT(zpu."Borough", ' / ', zpu."Zone"),CONCAT(zdo."Borough", ' / ', zdo."Zone")
ORDER BY C
```

## Question 6. 
```
SELECT 
	CAST(tpep_pickup_datetime AS DATE) as "day",
	CONCAT(zpu."Borough", ' / ', zpu."Zone") AS "pick_up_loc",
	CONCAT(zdo."Borough", ' / ', zdo."Zone") AS "dropoff_loc",
	AVG(t.total_amount)
FROM
	yellow_taxi_trips t JOIN zones zpu
		ON t."PULocationID" = zpu."LocationID"
	JOIN zones zdo
		ON t."DOLocationID" = zdo."LocationID"
group  by CAST(tpep_pickup_datetime AS DATE),CONCAT(zpu."Borough", ' / ', zpu."Zone"),CONCAT(zdo."Borough", ' / ', zdo."Zone")
ORDER BY AVG(t.total_amount) desc
limit 1;
```




