-- name: clean_trips
-- Clean the raw CSVs once and save them as Parquet; every later query reads that file.
-- Columns are read as text and cast with TRY_CAST, because their types differ across the monthly files.
COPY (
    WITH parsed AS (
        SELECT
            ride_id,
            rideable_type,
            TRY_CAST(started_at AS TIMESTAMP) AS started_at,
            TRY_CAST(ended_at AS TIMESTAMP) AS ended_at,
            start_station_name,
            NULLIF(TRIM(start_station_id), '') AS start_station_id,
            end_station_name,
            NULLIF(TRIM(end_station_id), '') AS end_station_id,
            TRY_CAST(start_lat AS DOUBLE) AS start_lat,
            TRY_CAST(start_lng AS DOUBLE) AS start_lng,
            TRY_CAST(end_lat AS DOUBLE) AS end_lat,
            TRY_CAST(end_lng AS DOUBLE) AS end_lng,
            member_casual
        FROM read_csv('data/raw/**/*.csv', all_varchar = true, union_by_name = true)
    )
    -- some IDs lost their trailing zero in the export ("5980.1" is station "5980.10"), so pad them back
    SELECT * REPLACE (
        CASE WHEN regexp_full_match(start_station_id, '\d+\.\d') THEN start_station_id || '0' ELSE start_station_id END AS start_station_id,
        CASE WHEN regexp_full_match(end_station_id, '\d+\.\d') THEN end_station_id || '0' ELSE end_station_id END AS end_station_id
    )
    FROM parsed
    WHERE ended_at - started_at BETWEEN INTERVAL 60 SECOND AND INTERVAL 6 HOUR
      -- keep only the 3 full calendar months (the June file also holds a few trips from late May)
      AND started_at >= (SELECT DATE_TRUNC('month', MAX(started_at)) - INTERVAL 2 MONTH FROM parsed)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ride_id ORDER BY started_at) = 1
) TO 'data/trips.parquet' (FORMAT parquet);

-- name: station_events
-- One row per bike leaving (-1) or arriving at (+1) a station; the station queries read from this view.
CREATE OR REPLACE VIEW station_events AS
SELECT start_station_id AS station_id, start_station_name AS name, start_lat AS lat, start_lng AS lng,
       started_at AS ts, -1 AS change
FROM 'data/trips.parquet' WHERE start_station_id IS NOT NULL
UNION ALL
SELECT end_station_id, end_station_name, end_lat, end_lng, ended_at, 1
FROM 'data/trips.parquet' WHERE end_station_id IS NOT NULL;

-- name: hourly_trips
-- Trips started per date and hour of day, for the whole system.
SELECT CAST(started_at AS DATE) AS date, HOUR(started_at) AS hour, COUNT(*) AS trips
FROM 'data/trips.parquet'
GROUP BY ALL
ORDER BY date, hour;

-- name: stations
-- One row per NYC station: most common name, median location, total trips, and net bikes gained 7-10 am on non-holiday weekdays.
SELECT station_id, MODE(name) AS name, MEDIAN(lat) AS lat, MEDIAN(lng) AS lng,
       COUNT(*) AS trips,  -- departures + arrivals
       SUM(change) FILTER (WHERE ISODOW(ts) <= 5 AND HOUR(ts) BETWEEN 7 AND 9
                           AND CAST(ts AS DATE) NOT IN (SELECT date FROM holidays)) AS morning_net_total
FROM station_events
WHERE station_id NOT LIKE 'JC%' AND station_id NOT LIKE 'HB%'  -- NJ stations only appear as trip ends in the NYC files
GROUP BY station_id
ORDER BY trips DESC;

-- name: daily_station_rides
-- Departures per station per date; trips with no end station still count. pandas zero-fills the missing days.
SELECT start_station_id AS station_id, CAST(started_at AS DATE) AS date, COUNT(*) AS rides
FROM 'data/trips.parquet'
WHERE start_station_id IS NOT NULL
GROUP BY ALL
ORDER BY station_id, date;
