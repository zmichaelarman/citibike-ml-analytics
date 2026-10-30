-- name: create_trips
-- the cleaned trips, stored as Parquet in one folder per month
CREATE EXTERNAL TABLE IF NOT EXISTS trips (
    ride_id STRING, rideable_type STRING,
    started_at TIMESTAMP, ended_at TIMESTAMP,
    start_station_name STRING, start_station_id STRING,
    end_station_name STRING, end_station_id STRING,
    start_lat DOUBLE, start_lng DOUBLE, end_lat DOUBLE, end_lng DOUBLE,
    member_casual STRING
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
LOCATION 's3://{bucket}/trips/'

-- name: register_partitions
-- adds the year=/month= folders to the catalog
MSCK REPAIR TABLE trips

-- name: create_trips_csv
-- one month of raw CSV, used only for the measurement; every column is text, in the CSV header's order
CREATE EXTERNAL TABLE IF NOT EXISTS trips_csv (
    ride_id STRING, rideable_type STRING, started_at STRING, ended_at STRING,
    start_station_name STRING, start_station_id STRING, end_station_name STRING, end_station_id STRING,
    start_lat STRING, start_lng STRING, end_lat STRING, end_lng STRING, member_casual STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION 's3://{bucket}/raw-csv/'
TBLPROPERTIES ('skip.header.line.count' = '1')

-- name: hourly_trips
SELECT date_trunc('day', started_at) AS "date", hour(started_at) AS hour, COUNT(*) AS trips
FROM trips
GROUP BY 1, 2
ORDER BY 1, 2

-- name: stations
-- same as the DuckDB version; Athena has no MODE or exact MEDIAN, so: most common name via max_by, approximate median
WITH events AS (
    SELECT start_station_id AS station_id, start_station_name AS name, start_lat AS lat, start_lng AS lng,
           started_at AS ts, -1 AS change
    FROM trips WHERE start_station_id IS NOT NULL
    UNION ALL
    SELECT end_station_id, end_station_name, end_lat, end_lng, ended_at, 1
    FROM trips WHERE end_station_id IS NOT NULL
),
name_counts AS (
    SELECT station_id, name, COUNT(*) AS n FROM events GROUP BY 1, 2
),
names AS (
    SELECT station_id, max_by(name, n) AS name FROM name_counts GROUP BY 1
)
SELECT e.station_id, n.name,
       approx_percentile(e.lat, 0.5) AS lat, approx_percentile(e.lng, 0.5) AS lng,
       COUNT(*) AS trips,
       SUM(e.change) FILTER (WHERE day_of_week(e.ts) <= 5 AND hour(e.ts) BETWEEN 7 AND 9
                             AND CAST(e.ts AS DATE) NOT IN ({holidays})) AS morning_net_total
FROM events e JOIN names n ON e.station_id = n.station_id
WHERE e.station_id NOT LIKE 'JC%' AND e.station_id NOT LIKE 'HB%'
GROUP BY e.station_id, n.name
ORDER BY trips DESC

-- name: daily_station_rides
SELECT start_station_id AS station_id, date_trunc('day', started_at) AS "date", COUNT(*) AS rides
FROM trips
WHERE start_station_id IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, 2

-- name: check_counts
-- months come from the timestamps, not the folder names, so a time-zone shift would show up as a mismatch
SELECT year(started_at) AS year, month(started_at) AS month, COUNT(*) AS trips FROM trips GROUP BY 1, 2 ORDER BY 1, 2

-- name: check_timestamps
SELECT started_at, ended_at FROM trips LIMIT 3

-- name: measure_csv
-- the same question three ways: June trips per hour
-- (raw rows before cleaning, so the counts differ slightly; only the data scanned is compared)
SELECT hour(try_cast(started_at AS timestamp)) AS hour, COUNT(*) AS trips
FROM trips_csv
WHERE started_at LIKE '2026-06%'
GROUP BY 1
ORDER BY 1

-- name: measure_parquet
SELECT hour(started_at) AS hour, COUNT(*) AS trips
FROM trips
WHERE year = 2026 AND month = 6
GROUP BY 1
ORDER BY 1

-- name: measure_parquet_no_partition
SELECT hour(started_at) AS hour, COUNT(*) AS trips
FROM trips
WHERE started_at >= TIMESTAMP '2026-06-01 00:00:00' AND started_at < TIMESTAMP '2026-07-01 00:00:00'
GROUP BY 1
ORDER BY 1
