## Week 5 Homework
## Question 1. Install Spark and PySpark

* Install Spark
* Run PySpark
* Create a local spark session 
* Execute `spark.version`

What's the output?

```3.0.3```


## Question 2. HVFHW February 2021

Download the HVFHV data for february 2021:

```bash
wget https://nyc-tlc.s3.amazonaws.com/trip+data/fhvhv_tripdata_2021-02.csv
```

Read it with Spark using the same schema as we did 
in the lessons. We will use this dataset for all
the remaining questions.

Repartition it to 24 partitions and save it to
parquet.

What's the size of the folder with results (in MB)?

```
df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv('/content/fhvhv_tripdata_2021-02.csv')

df.repartition(24).write.parquet('parquet/')
```
```208```
## Question 3. Count records 

How many taxi trips were there on February 15?

Consider only trips that started on February 15.

```
spark.sql("""
SELECT 
    COUNT(*) AS trips
FROM 
    trips_data
WHERE 
    pickup_date == '2021-02-15'
""").show()
```
```
+------+
| trips|
+------+
|367170|
+------+
```
## Question 4. Longest trip for each day

Now calculate the duration for each trip.

Trip starting on which day was the longest? 

```
spark.sql("""
SELECT 
    pickup_date,
    pickup_datetime,
    dropoff_datetime,
    (dropoff_datetime - pickup_datetime) AS trip_duration,
    (BIGINT(dropoff_datetime) - BIGINT(pickup_datetime)) AS trip_duration_int
FROM 
    trips_data
ORDER BY 
    trip_duration_int DESC
""").show(3)
```

```
+-----------+-------------------+-------------------+--------------------+-----------------+
|pickup_date|    pickup_datetime|   dropoff_datetime|       trip_duration|trip_duration_int|
+-----------+-------------------+-------------------+--------------------+-----------------+
| 2021-02-11|2021-02-11 13:40:44|2021-02-12 10:39:44| 20 hours 59 minutes|            75540|
```
## Question 5. Most frequent `dispatching_base_num`

Now find the most frequently occurring `dispatching_base_num` 
in this dataset.

How many stages this spark job has?


```
spark.sql("""
SELECT 
    dispatching_base_num,
    COUNT(*) AS trips
FROM 
    trips_data
GROUP BY 
    dispatching_base_num
ORDER BY 
    trips DESC
""").show(5)
```
How many stages this spark job has?
```2```
## Question 6. Most common locations pair

Find the most common pickup-dropoff pair. 

For example:

"Jamaica Bay / Clinton East"

Enter two zone names separated by a slash

If any of the zone names are unknown (missing), use "Unknown". For example, "Unknown / Clinton East". 


```
spark.sql("""
SELECT 
    CONCAT(COALESCE(zpuZone, 'Unknown'), '/', COALESCE(zdoZone, 'Unknown')) AS locations,
    COUNT(1) AS trips
FROM 
    locations_table
GROUP BY 
    locations
ORDER BY 
    trips DESC
""").show(5)
```
```
+--------------------+-----+
|           locations|trips|
+--------------------+-----+
|East New York/Eas...|45041|
|Borough Park/Boro...|37329|
|   Canarsie/Canarsie|28026|
|Crown Heights Nor...|25976|
| Bay Ridge/Bay Ridge|17934|
```